"""
HR & Payroll REST API
=====================
Connects to a SQL Server database restored from bishift.bak and exposes
endpoints for the HR and Payroll schemas.

Schemas covered
---------------
  HR      : employees, shifts, attendance_events, leave_requests
  Payroll : compensation_packages, compensation_changes, payroll_runs

Setup
-----
1. Install dependencies:
       pip install fastapi uvicorn pyodbc python-dotenv

2. Create a .env file (or set environment variables):
       DB_SERVER=localhost
       DB_NAME=bishift
       DB_USER=sa
       DB_PASSWORD=YourPassword123

3. Restore the backup in SQL Server:
       RESTORE DATABASE bishift
         FROM DISK = '/path/to/bishift.bak'
         WITH MOVE 'bishift'     TO '/var/opt/mssql/data/bishift.mdf',
              MOVE 'bishift_log' TO '/var/opt/mssql/data/bishift_log.ldf';

4. Run the API:
       uvicorn api_hr_payroll:app --reload --port 8000

Interactive docs available at: http://localhost:8000/docs
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

import pyodbc
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

load_dotenv()

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

DB_SERVER   = os.getenv("DB_SERVER", "localhost")
DB_NAME     = os.getenv("DB_NAME",   "bishift")
DB_USER     = os.getenv("DB_USER",   "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    "TrustServerCertificate=yes;"
)


def get_conn():
    """Yield a pyodbc connection; close it when the request is done."""
    conn = pyodbc.connect(CONN_STR)
    try:
        yield conn
    finally:
        conn.close()


def rows_to_dicts(cursor) -> list[dict]:
    """Convert cursor rows to a list of dicts using column names."""
    cols = [col[0] for col in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify DB connectivity at startup
    try:
        conn = pyodbc.connect(CONN_STR, timeout=5)
        conn.close()
        print("✅  Database connection OK")
    except Exception as exc:
        print(f"⚠️  Could not connect to database: {exc}")
    yield


app = FastAPI(
    title="BiShift — HR & Payroll API",
    description=(
        "REST API for the HR and Payroll schemas of the BiShift SQL Server database. "
        "Covers employees, shifts, attendance, leave requests, compensation, and payroll runs."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
def health_check(conn=Depends(get_conn)):
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION AS version")
    row = cursor.fetchone()
    return {"status": "ok", "sql_server_version": row[0].split("\n")[0]}


# ===========================================================================
# HR — Employees
# ===========================================================================

@app.get("/hr/employees", tags=["HR – Employees"])
def list_employees(
    limit: int = Query(50, ge=1, le=1000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip (pagination)"),
    name: Optional[str] = Query(None, description="Filter by name (partial match)"),
    conn=Depends(get_conn),
):
    """
    Return a paginated list of employees.

    Optional filter: **name** (case-insensitive partial match on the name column).
    """
    cursor = conn.cursor()
    if name:
        cursor.execute(
            """
            SELECT * FROM dbo.employees
            WHERE name LIKE ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            f"%{name}%", offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.employees
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/hr/employees/{employee_id}", tags=["HR – Employees"])
def get_employee(employee_id: int, conn=Depends(get_conn)):
    """Return a single employee by primary key."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.employees WHERE id = ?", employee_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Employee {employee_id} not found")
    return data[0]


@app.get("/hr/employees/{employee_id}/shifts", tags=["HR – Employees"])
def get_employee_shifts(
    employee_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return all shifts assigned to a specific employee."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT s.* FROM dbo.shifts s
        WHERE s.employee_id = ?
        ORDER BY s.start_time DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        employee_id, offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"employee_id": employee_id, "count": len(data), "data": data}


@app.get("/hr/employees/{employee_id}/attendance", tags=["HR – Employees"])
def get_employee_attendance(
    employee_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return attendance events for a specific employee."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.attendance_events
        WHERE employee_id = ?
        ORDER BY event_time DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        employee_id, offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"employee_id": employee_id, "count": len(data), "data": data}


# ===========================================================================
# HR — Shifts
# ===========================================================================

@app.get("/hr/shifts", tags=["HR – Shifts"])
def list_shifts(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return a paginated list of all shifts."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.shifts
        ORDER BY (SELECT NULL)
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/hr/shifts/{shift_id}", tags=["HR – Shifts"])
def get_shift(shift_id: int, conn=Depends(get_conn)):
    """Return a single shift by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.shifts WHERE id = ?", shift_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Shift {shift_id} not found")
    return data[0]


# ===========================================================================
# HR — Attendance Events
# ===========================================================================

@app.get("/hr/attendance", tags=["HR – Attendance"])
def list_attendance(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return a paginated list of attendance events (all employees)."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.attendance_events
        ORDER BY event_time DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


# ===========================================================================
# HR — Leave Requests
# ===========================================================================

@app.get("/hr/leave-requests", tags=["HR – Leave"])
def list_leave_requests(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, description="Filter by status (e.g. pending, approved, rejected)"),
    conn=Depends(get_conn),
):
    """Return a paginated list of leave requests with optional status filter."""
    cursor = conn.cursor()
    if status:
        cursor.execute(
            """
            SELECT * FROM dbo.leave_requests
            WHERE status = ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            status, offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.leave_requests
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/hr/leave-requests/{request_id}", tags=["HR – Leave"])
def get_leave_request(request_id: int, conn=Depends(get_conn)):
    """Return a single leave request by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.leave_requests WHERE id = ?", request_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Leave request {request_id} not found")
    return data[0]


# ===========================================================================
# Payroll — Compensation Packages
# ===========================================================================

@app.get("/payroll/compensation-packages", tags=["Payroll – Compensation"])
def list_compensation_packages(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return all compensation packages."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.compensation_packages
        ORDER BY (SELECT NULL)
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/payroll/compensation-packages/{package_id}", tags=["Payroll – Compensation"])
def get_compensation_package(package_id: int, conn=Depends(get_conn)):
    """Return a single compensation package by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.compensation_packages WHERE id = ?", package_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Compensation package {package_id} not found")
    return data[0]


@app.get("/payroll/compensation-changes", tags=["Payroll – Compensation"])
def list_compensation_changes(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    employee_id: Optional[int] = Query(None, description="Filter by employee ID"),
    conn=Depends(get_conn),
):
    """Return compensation change history, optionally filtered by employee."""
    cursor = conn.cursor()
    if employee_id:
        cursor.execute(
            """
            SELECT * FROM dbo.compensation_changes
            WHERE employee_id = ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            employee_id, offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.compensation_changes
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


# ===========================================================================
# Payroll — Payroll Runs
# ===========================================================================

@app.get("/payroll/runs", tags=["Payroll – Runs"])
def list_payroll_runs(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return a list of payroll runs ordered by most recent first."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.payroll_runs
        ORDER BY (SELECT NULL)
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/payroll/runs/{run_id}", tags=["Payroll – Runs"])
def get_payroll_run(run_id: int, conn=Depends(get_conn)):
    """Return a single payroll run by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.payroll_runs WHERE id = ?", run_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Payroll run {run_id} not found")
    return data[0]


# ===========================================================================
# Summary / reporting endpoints
# ===========================================================================

@app.get("/hr/summary", tags=["HR – Reports"])
def hr_summary(conn=Depends(get_conn)):
    """
    Returns high-level counts for all HR tables — useful for dashboards.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM dbo.employees)         AS total_employees,
            (SELECT COUNT(*) FROM dbo.shifts)            AS total_shifts,
            (SELECT COUNT(*) FROM dbo.attendance_events) AS total_attendance_events,
            (SELECT COUNT(*) FROM dbo.leave_requests)    AS total_leave_requests
        """
    )
    row = cursor.fetchone()
    return {
        "total_employees":         row[0],
        "total_shifts":            row[1],
        "total_attendance_events": row[2],
        "total_leave_requests":    row[3],
    }


@app.get("/payroll/summary", tags=["Payroll – Reports"])
def payroll_summary(conn=Depends(get_conn)):
    """
    Returns high-level counts for all Payroll tables.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM dbo.compensation_packages) AS total_compensation_packages,
            (SELECT COUNT(*) FROM dbo.compensation_changes)  AS total_compensation_changes,
            (SELECT COUNT(*) FROM dbo.payroll_runs)          AS total_payroll_runs
        """
    )
    row = cursor.fetchone()
    return {
        "total_compensation_packages": row[0],
        "total_compensation_changes":  row[1],
        "total_payroll_runs":          row[2],
    }
