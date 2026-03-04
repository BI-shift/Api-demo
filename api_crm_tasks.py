"""
CRM & Tasks REST API
====================
Connects to a SQL Server database restored from bishift.bak and exposes
endpoints for the CRM and Task schemas.

Schemas covered
---------------
  CRM  : crm_accounts, crm_contacts, crm_deals, crm_activities
  Task : projects, work_items, work_item_events

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
       uvicorn api_crm_tasks:app --reload --port 8001

Interactive docs available at: http://localhost:8001/docs
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

import pyodbc
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query

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
    try:
        conn = pyodbc.connect(CONN_STR, timeout=5)
        conn.close()
        print("✅  Database connection OK")
    except Exception as exc:
        print(f"⚠️  Could not connect to database: {exc}")
    yield


app = FastAPI(
    title="BiShift — CRM & Tasks API",
    description=(
        "REST API for the CRM and Task schemas of the BiShift SQL Server database. "
        "Covers CRM accounts, contacts, deals, activities, projects, work items, and events."
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
# CRM — Accounts
# ===========================================================================

@app.get("/crm/accounts", tags=["CRM – Accounts"])
def list_crm_accounts(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    name: Optional[str] = Query(None, description="Filter by account name (partial match)"),
    conn=Depends(get_conn),
):
    """Return a paginated list of CRM accounts."""
    cursor = conn.cursor()
    if name:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_accounts
            WHERE name LIKE ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            f"%{name}%", offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_accounts
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/crm/accounts/{account_id}", tags=["CRM – Accounts"])
def get_crm_account(account_id: int, conn=Depends(get_conn)):
    """Return a single CRM account by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.crm_accounts WHERE id = ?", account_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Account {account_id} not found")
    return data[0]


@app.get("/crm/accounts/{account_id}/contacts", tags=["CRM – Accounts"])
def get_account_contacts(
    account_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return all contacts linked to a CRM account."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.crm_contacts
        WHERE account_id = ?
        ORDER BY (SELECT NULL)
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        account_id, offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"account_id": account_id, "count": len(data), "data": data}


@app.get("/crm/accounts/{account_id}/deals", tags=["CRM – Accounts"])
def get_account_deals(
    account_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return all deals linked to a CRM account."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.crm_deals
        WHERE account_id = ?
        ORDER BY (SELECT NULL)
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        account_id, offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"account_id": account_id, "count": len(data), "data": data}


# ===========================================================================
# CRM — Contacts
# ===========================================================================

@app.get("/crm/contacts", tags=["CRM – Contacts"])
def list_crm_contacts(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    email: Optional[str] = Query(None, description="Filter by email (partial match)"),
    conn=Depends(get_conn),
):
    """Return a paginated list of CRM contacts."""
    cursor = conn.cursor()
    if email:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_contacts
            WHERE email LIKE ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            f"%{email}%", offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_contacts
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/crm/contacts/{contact_id}", tags=["CRM – Contacts"])
def get_crm_contact(contact_id: int, conn=Depends(get_conn)):
    """Return a single CRM contact by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.crm_contacts WHERE id = ?", contact_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Contact {contact_id} not found")
    return data[0]


# ===========================================================================
# CRM — Deals
# ===========================================================================

@app.get("/crm/deals", tags=["CRM – Deals"])
def list_crm_deals(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    stage: Optional[str] = Query(None, description="Filter by deal stage"),
    conn=Depends(get_conn),
):
    """Return a paginated list of CRM deals, optionally filtered by stage."""
    cursor = conn.cursor()
    if stage:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_deals
            WHERE stage = ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            stage, offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_deals
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/crm/deals/{deal_id}", tags=["CRM – Deals"])
def get_crm_deal(deal_id: int, conn=Depends(get_conn)):
    """Return a single CRM deal by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.crm_deals WHERE id = ?", deal_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Deal {deal_id} not found")
    return data[0]


# ===========================================================================
# CRM — Activities
# ===========================================================================

@app.get("/crm/activities", tags=["CRM – Activities"])
def list_crm_activities(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    employee_id: Optional[int] = Query(None, description="Filter by employee (owner)"),
    conn=Depends(get_conn),
):
    """Return CRM activities. Optionally filter by the owning employee."""
    cursor = conn.cursor()
    if employee_id:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_activities
            WHERE employee_id = ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            employee_id, offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.crm_activities
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/crm/activities/{activity_id}", tags=["CRM – Activities"])
def get_crm_activity(activity_id: int, conn=Depends(get_conn)):
    """Return a single CRM activity by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.crm_activities WHERE id = ?", activity_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Activity {activity_id} not found")
    return data[0]


# ===========================================================================
# Tasks — Projects
# ===========================================================================

@app.get("/tasks/projects", tags=["Tasks – Projects"])
def list_projects(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    name: Optional[str] = Query(None, description="Filter by project name (partial match)"),
    conn=Depends(get_conn),
):
    """Return a paginated list of projects."""
    cursor = conn.cursor()
    if name:
        cursor.execute(
            """
            SELECT * FROM dbo.projects
            WHERE name LIKE ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            f"%{name}%", offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.projects
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/tasks/projects/{project_id}", tags=["Tasks – Projects"])
def get_project(project_id: int, conn=Depends(get_conn)):
    """Return a single project by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.projects WHERE id = ?", project_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Project {project_id} not found")
    return data[0]


@app.get("/tasks/projects/{project_id}/work-items", tags=["Tasks – Projects"])
def get_project_work_items(
    project_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return all work items belonging to a project."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.work_items
        WHERE project_id = ?
        ORDER BY (SELECT NULL)
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        project_id, offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"project_id": project_id, "count": len(data), "data": data}


# ===========================================================================
# Tasks — Work Items
# ===========================================================================

@app.get("/tasks/work-items", tags=["Tasks – Work Items"])
def list_work_items(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    employee_id: Optional[int] = Query(None, description="Filter by assigned employee"),
    conn=Depends(get_conn),
):
    """Return a paginated list of work items, optionally filtered by assignee."""
    cursor = conn.cursor()
    if employee_id:
        cursor.execute(
            """
            SELECT * FROM dbo.work_items
            WHERE employee_id = ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            employee_id, offset, limit,
        )
    else:
        cursor.execute(
            """
            SELECT * FROM dbo.work_items
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            offset, limit,
        )
    data = rows_to_dicts(cursor)
    return {"count": len(data), "offset": offset, "limit": limit, "data": data}


@app.get("/tasks/work-items/{item_id}", tags=["Tasks – Work Items"])
def get_work_item(item_id: int, conn=Depends(get_conn)):
    """Return a single work item by ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.work_items WHERE id = ?", item_id)
    data = rows_to_dicts(cursor)
    if not data:
        raise HTTPException(status_code=404, detail=f"Work item {item_id} not found")
    return data[0]


@app.get("/tasks/work-items/{item_id}/events", tags=["Tasks – Work Items"])
def get_work_item_events(
    item_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Return all events (status changes, comments, etc.) for a work item."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM dbo.work_item_events
        WHERE work_item_id = ?
        ORDER BY (SELECT NULL)
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """,
        item_id, offset, limit,
    )
    data = rows_to_dicts(cursor)
    return {"work_item_id": item_id, "count": len(data), "data": data}


# ===========================================================================
# Summary / reporting endpoints
# ===========================================================================

@app.get("/crm/summary", tags=["CRM – Reports"])
def crm_summary(conn=Depends(get_conn)):
    """High-level counts for all CRM tables — useful for dashboards."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM dbo.crm_accounts)    AS total_accounts,
            (SELECT COUNT(*) FROM dbo.crm_contacts)    AS total_contacts,
            (SELECT COUNT(*) FROM dbo.crm_deals)       AS total_deals,
            (SELECT COUNT(*) FROM dbo.crm_activities)  AS total_activities
        """
    )
    row = cursor.fetchone()
    return {
        "total_accounts":   row[0],
        "total_contacts":   row[1],
        "total_deals":      row[2],
        "total_activities": row[3],
    }


@app.get("/tasks/summary", tags=["Tasks – Reports"])
def tasks_summary(conn=Depends(get_conn)):
    """High-level counts for all Task tables."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM dbo.projects)          AS total_projects,
            (SELECT COUNT(*) FROM dbo.work_items)        AS total_work_items,
            (SELECT COUNT(*) FROM dbo.work_item_events)  AS total_work_item_events
        """
    )
    row = cursor.fetchone()
    return {
        "total_projects":         row[0],
        "total_work_items":       row[1],
        "total_work_item_events": row[2],
    }
