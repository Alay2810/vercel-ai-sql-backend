const mysql = require("mysql2");

const db = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

db.connect(err => {
  if (err) {
    console.error("❌ MySQL connection failed:", err.message);
    console.error("Check your .env file and ensure MySQL is running");
    process.exit(1);
  }
  console.log("✅ MySQL Connected Successfully");
});

module.exports = db;
