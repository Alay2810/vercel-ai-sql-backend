const mysql = require("mysql2");

// Create connection pool for serverless
const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Test connection (only runs in dev)
if (process.env.NODE_ENV !== 'production') {
  db.getConnection((err, connection) => {
    if (err) {
      console.error("❌ MySQL connection failed:", err.message);
      console.error("Check your .env file and ensure MySQL is running");
    } else {
      console.log("✅ MySQL Connected Successfully");
      connection.release();
    }
  });
}

module.exports = db;
