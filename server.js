const express = require("express");
const fs = require("fs");
const app = express();
const PORT = process.env.PORT || 3000;

// Load mock customer data
const customers = JSON.parse(fs.readFileSync("./customers.json", "utf8"));

// API endpoint: GET /customer/:code
app.get("/customer/:code", (req, res) => {
  const code = req.params.code.toUpperCase();

  if (customers[code]) {
    return res.json({
      success: true,
      data: customers[code]
    });
  }

  return res.status(404).json({
    success: false,
    message: "Customer not found"
  });
});

app.listen(PORT, () => {
  console.log(`Demo API running on port ${PORT}`);
});
