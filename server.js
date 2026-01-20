require("dotenv").config();
const express = require("express");
const cors = require("cors");
const Groq = require("groq-sdk");
const db = require("./db");
const multer = require("multer");
const csv = require("csv-parser");
const fs = require("fs");
const XLSX = require("xlsx");
const path = require("path");

const app = express();
app.use(cors({
  origin: [
    'http://localhost:3000',
    'https://ai-sql-agent-frontend.vercel.app',
    'https://ai-sql-agent-frontend-f8ks2e7j4-alay2810s-projects.vercel.app'
  ],
  credentials: true
}));
app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok', 
    message: 'AI SQL Backend is running',
    timestamp: new Date().toISOString()
  });
});

app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    database: db ? 'connected' : 'disconnected',
    groq: process.env.GROQ_API_KEY ? 'configured' : 'missing'
  });
});

// Configure multer
const upload = multer({ dest: "uploads/" });

const groq = new Groq({
  apiKey: process.env.GROQ_API_KEY
});

// Get table schema
function getSchema(tableName) {
  return new Promise((resolve, reject) => {
    const query = `
      SELECT COLUMN_NAME, DATA_TYPE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
    `;
    db.query(query, [tableName, process.env.DB_NAME], (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Format schema
function formatSchema(schema, tableName) {
  return `${tableName}(` +
    schema.map(col => `${col.COLUMN_NAME} ${col.DATA_TYPE}`).join(", ")
    + `)`;
}

// Get all tables
function getAllTables() {
  return new Promise((resolve, reject) => {
    const query = `
      SELECT 
        TABLE_NAME as tableName,
        CREATE_TIME as createdAt,
        TABLE_ROWS as rowCount
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = ?
      ORDER BY CREATE_TIME DESC
    `;
    db.query(query, [process.env.DB_NAME], (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Get table preview
function getTablePreview(tableName) {
  return new Promise((resolve, reject) => {
    const query = `SELECT * FROM ?? LIMIT 10`;
    db.query(query, [tableName], (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Parse CSV
function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => rows.push(data))
      .on("end", () => resolve(rows))
      .on("error", (err) => reject(err));
  });
}

// Parse Excel
function parseExcel(filePath) {
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(worksheet);
  return rows;
}

// Parse JSON
function parseJSON(filePath) {
  const data = fs.readFileSync(filePath, "utf8");
  const rows = JSON.parse(data);
  return Array.isArray(rows) ? rows : [rows];
}

// Get list of tables
app.get("/tables", async (req, res) => {
  try {
    console.log("📋 Fetching table list...");
    const tables = await getAllTables();
    
    const tablesWithDetails = await Promise.all(
      tables.map(async (table) => {
        const schema = await getSchema(table.tableName);
        return {
          ...table,
          columnCount: schema.length,
          columns: schema.map(col => col.COLUMN_NAME)
        };
      })
    );
    
    console.log(`✅ Found ${tablesWithDetails.length} tables`);
    res.json({ tables: tablesWithDetails });
  } catch (err) {
    console.error("❌ Error fetching tables:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get table preview
app.get("/table/:tableName/preview", async (req, res) => {
  const { tableName } = req.params;
  
  try {
    console.log(`🔍 Loading preview for table: ${tableName}`);
    const schema = await getSchema(tableName);
    const preview = await getTablePreview(tableName);
    
    res.json({
      tableName,
      schema,
      preview,
      columnCount: schema.length,
      rowCount: preview.length
    });
  } catch (err) {
    console.error(`❌ Error loading preview for ${tableName}:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Upload file
app.post("/upload", upload.single("file"), async (req, res) => {
  const tableName = req.body.tableName;
  const filePath = req.file?.path;
  const fileExt = req.file?.originalname?.split(".").pop().toLowerCase();

  console.log("📤 Upload request:", { tableName, filePath, fileExt });

  if (!tableName || !filePath) {
    console.log("❌ Missing tableName or filePath");
    return res.status(400).json({ error: "Table name and file required" });
  }

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    console.log("❌ Invalid table name:", tableName);
    fs.unlinkSync(filePath);
    return res.status(400).json({ error: `Invalid table name: ${tableName}. Use only letters, numbers, and underscores.` });
  }

  try {
    let rows = [];

    if (fileExt === "csv") {
      rows = await parseCSV(filePath);
    } else if (fileExt === "xlsx" || fileExt === "xls") {
      rows = parseExcel(filePath);
    } else if (fileExt === "json") {
      rows = parseJSON(filePath);
    } else {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: "Unsupported file format" });
    }

    if (rows.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: "File is empty" });
    }

    const columns = Object.keys(rows[0]);

    // Drop and create table
    await new Promise((resolve, reject) => {
      db.query(`DROP TABLE IF EXISTS ??`, [tableName], (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    const createTableSQL = `
      CREATE TABLE ?? (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ${columns.map(() => `?? VARCHAR(255)`).join(",")}
      )
    `;

    await new Promise((resolve, reject) => {
      db.query(createTableSQL, [tableName, ...columns], (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    // Insert data
    const insertPromises = rows.map((row) => {
      return new Promise((resolve, reject) => {
        const values = columns.map(col => row[col]);
        const placeholders = columns.map(() => '?').join(',');
        const insertSQL = `INSERT INTO ?? (${columns.map(() => '??').join(',')}) VALUES (${placeholders})`;
        
        db.query(insertSQL, [tableName, ...columns, ...values], (err) => {
          if (err) reject(err);
          else resolve();
        });
      });
    });

    await Promise.all(insertPromises);
    fs.unlinkSync(filePath);

    console.log(`✅ Table '${tableName}' created with ${rows.length} rows`);
    
    res.json({
      message: `Table '${tableName}' created successfully`,
      tableName,
      rowCount: rows.length,
      columnCount: columns.length,
      columns
    });

  } catch (err) {
    console.error("❌ Upload error:", err);
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    res.status(500).json({ error: err.message });
  }
});

// Ask question (Multi-table support)
app.post("/ask", async (req, res) => {
  const { tables, question } = req.body;

  // Support legacy single table format
  const tableList = tables ? (Array.isArray(tables) ? tables : [tables]) : 
                    (req.body.table ? [req.body.table] : []);

  console.log("📨 Ask request:", { tables: tableList, question });

  if (tableList.length === 0 || !question) {
    return res.status(400).json({ error: "At least one table and question required" });
  }

  try {
    // Get schemas for all selected tables
    const schemasPromises = tableList.map(table => getSchema(table));
    const schemasResults = await Promise.all(schemasPromises);
    
    // Format all schemas
    const schemasText = tableList.map((table, idx) => {
      if (!schemasResults[idx] || schemasResults[idx].length === 0) {
        throw new Error(`Table '${table}' not found`);
      }
      return formatSchema(schemasResults[idx], table);
    }).join("\n");

    const prompt = `
You are an expert MySQL SQL generator.

Rules:
- Generate ONLY SQL code, no explanations in the query
- Use ONLY the schemas provided below
- Use MySQL syntax
- For multi-table queries, use JOIN, UNION, or subqueries as appropriate
- Do NOT add LIMIT unless explicitly requested
- Do NOT hallucinate columns or tables
- Generate SELECT queries for read operations
- Generate INSERT/UPDATE/DELETE only if explicitly requested
- Warn about destructive operations

Available Tables and Schemas:
${schemasText}

Question:
${question}

Output format:
SQL_QUERY:
<sql>

BUSINESS_EXPLANATION:
<explanation>

WARNING:
<warning if destructive operation>
`;

    console.log("🤖 Calling Groq AI...");
    const response = await groq.chat.completions.create({
      model: "llama-3.3-70b-versatile",
      messages: [
        { role: "system", content: "You are an expert MySQL SQL generator. Generate only valid MySQL queries." },
        { role: "user", content: prompt }
      ],
      temperature: 0
    });

    const content = response.choices[0].message.content;

    let sql = content
      .split("SQL_QUERY:")[1]
      ?.split("BUSINESS_EXPLANATION:")[0]
      ?.split("WARNING:")[0]
      ?.trim() || "";

    sql = sql.replace(/```sql\n?/g, "").replace(/```\n?/g, "").trim();

    let explanation = content
      .split("BUSINESS_EXPLANATION:")[1]
      ?.split("WARNING:")[0]
      ?.trim() || "No explanation provided";

    let warning = content.split("WARNING:")[1]?.trim() || "";

    // Check for destructive operations
    const sqlLower = sql.toLowerCase();
    const isDestructive = sqlLower.includes("delete") || sqlLower.includes("drop") || 
                         sqlLower.includes("truncate") || sqlLower.includes("update");
    
    if (isDestructive && !warning) {
      warning = "⚠️ This query will modify or delete data. Review carefully before executing.";
    }

    console.log("🔍 Executing SQL:", sql);

    db.query(sql, (err, results) => {
      if (err) {
        console.error("❌ SQL Error:", err.message);
        return res.status(400).json({ error: err.message });
      }

      console.log(`✅ Query successful, ${results.length || 0} rows affected`);
      res.json({
        sql,
        results: Array.isArray(results) ? results : [],
        explanation,
        warning,
        rowCount: Array.isArray(results) ? results.length : 0,
        affectedRows: results.affectedRows || 0
      });
    });

  } catch (err) {
    console.error("❌ Server Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Export to Excel
app.post("/export", (req, res) => {
  const { data, tableName } = req.body;

  if (!data || data.length === 0) {
    return res.status(400).json({ error: "No data to export" });
  }

  try {
    const worksheet = XLSX.utils.json_to_sheet(data);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Results");

    const fileName = `${tableName || "query"}_${Date.now()}.xlsx`;
    const filePath = path.join(__dirname, "uploads", fileName);

    XLSX.writeFile(workbook, filePath);

    res.download(filePath, fileName, (err) => {
      if (err) console.error("Download error:", err);
      fs.unlinkSync(filePath);
    });

  } catch (err) {
    console.error("❌ Export error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get full table data with pagination
app.get("/table/:tableName/full", async (req, res) => {
  const { tableName } = req.params;
  const { offset = 0, limit = 1000 } = req.query;
  
  try {
    console.log(`📊 Fetching full table: ${tableName} (offset: ${offset}, limit: ${limit})`);
    
    // Get total count
    const countQuery = `SELECT COUNT(*) as total FROM ??`;
    const countResult = await new Promise((resolve, reject) => {
      db.query(countQuery, [tableName], (err, rows) => {
        if (err) reject(err);
        else resolve(rows[0].total);
      });
    });
    
    // Get data
    const dataQuery = `SELECT * FROM ?? LIMIT ? OFFSET ?`;
    const data = await new Promise((resolve, reject) => {
      db.query(dataQuery, [tableName, parseInt(limit), parseInt(offset)], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });
    
    // Get schema
    const schema = await getSchema(tableName);
    
    res.json({
      tableName,
      data,
      schema,
      total: countResult,
      offset: parseInt(offset),
      limit: parseInt(limit),
      hasMore: (parseInt(offset) + data.length) < countResult
    });
    
  } catch (err) {
    console.error(`❌ Error fetching full table ${tableName}:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Download table or query result as CSV/Excel
app.post("/download", (req, res) => {
  const { data, tableName, format = 'xlsx' } = req.body;

  if (!data || data.length === 0) {
    return res.status(400).json({ error: "No data to download" });
  }

  try {
    const fileName = `${tableName || "query_result"}_${Date.now()}`;
    
    if (format === 'csv') {
      // Generate CSV
      const headers = Object.keys(data[0]).join(',');
      const rows = data.map(row => 
        Object.values(row).map(val => 
          typeof val === 'string' ? `"${val.replace(/"/g, '""')}"` : val
        ).join(',')
      );
      const csv = [headers, ...rows].join('\n');
      
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="${fileName}.csv"`);
      res.send(csv);
    } else {
      // Generate Excel
      const worksheet = XLSX.utils.json_to_sheet(data);
      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, worksheet, "Data");

      const filePath = path.join(__dirname, "uploads", `${fileName}.xlsx`);
      XLSX.writeFile(workbook, filePath);

      res.download(filePath, `${fileName}.xlsx`, (err) => {
        if (err) console.error("Download error:", err);
        fs.unlinkSync(filePath);
      });
    }

  } catch (err) {
    console.error("❌ Download error:", err);
    res.status(500).json({ error: err.message });
  }
});

// CRUD Operations
app.post("/crud", async (req, res) => {
  const { operation, tableName, data, where } = req.body;
  
  console.log("🔧 CRUD request:", { operation, tableName });
  
  if (!operation || !tableName) {
    return res.status(400).json({ error: "Operation and table name required" });
  }
  
  try {
    let sql, params;
    
    switch (operation.toUpperCase()) {
      case 'INSERT':
        if (!data || Object.keys(data).length === 0) {
          return res.status(400).json({ error: "Data required for INSERT" });
        }
        const columns = Object.keys(data);
        const values = Object.values(data);
        const placeholders = columns.map(() => '?').join(',');
        sql = `INSERT INTO ?? (${columns.map(() => '??').join(',')}) VALUES (${placeholders})`;
        params = [tableName, ...columns, ...values];
        break;
        
      case 'UPDATE':
        if (!data || Object.keys(data).length === 0) {
          return res.status(400).json({ error: "Data required for UPDATE" });
        }
        if (!where) {
          return res.status(400).json({ error: "WHERE clause required for UPDATE" });
        }
        const setClauses = Object.keys(data).map(col => `?? = ?`);
        sql = `UPDATE ?? SET ${setClauses.join(', ')} WHERE ${where}`;
        params = [tableName, ...Object.entries(data).flat()];
        break;
        
      case 'DELETE':
        if (!where) {
          return res.status(400).json({ error: "WHERE clause required for DELETE" });
        }
        sql = `DELETE FROM ?? WHERE ${where}`;
        params = [tableName];
        break;
        
      case 'TRUNCATE':
        sql = `TRUNCATE TABLE ??`;
        params = [tableName];
        break;
        
      case 'DROP':
        sql = `DROP TABLE IF EXISTS ??`;
        params = [tableName];
        break;
        
      default:
        return res.status(400).json({ error: `Unsupported operation: ${operation}` });
    }
    
    db.query(sql, params, (err, result) => {
      if (err) {
        console.error("❌ CRUD Error:", err.message);
        return res.status(400).json({ error: err.message });
      }
      
      console.log(`✅ ${operation} successful`);
      res.json({
        success: true,
        operation,
        affectedRows: result.affectedRows || 0,
        message: `${operation} completed successfully`
      });
    });
    
  } catch (err) {
    console.error("❌ CRUD error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Execute raw SQL (with safety checks)
app.post("/execute", async (req, res) => {
  const { sql, requireConfirmation } = req.body;
  
  if (!sql) {
    return res.status(400).json({ error: "SQL query required" });
  }
  
  console.log("⚡ Execute SQL:", sql);
  
  try {
    db.query(sql, (err, results) => {
      if (err) {
        console.error("❌ SQL Error:", err.message);
        return res.status(400).json({ error: err.message });
      }
      
      console.log(`✅ SQL executed, ${results.length || 0} rows`);
      res.json({
        results: Array.isArray(results) ? results : [],
        rowCount: Array.isArray(results) ? results.length : 0,
        affectedRows: results.affectedRows || 0
      });
    });
  } catch (err) {
    console.error("❌ Execute error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get table row count
app.get("/table/:tableName/count", async (req, res) => {
  const { tableName } = req.params;
  
  try {
    const query = `SELECT COUNT(*) as count FROM ??`;
    db.query(query, [tableName], (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json({ tableName, count: rows[0].count });
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Health check
app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date() });
});

// Start server
app.listen(5000, "0.0.0.0", () => {
  console.log("=================================");
  console.log("🚀 AI SQL Workspace Backend");
  console.log("=================================");
  console.log("Server: http://localhost:5000");
  console.log("\nEndpoints:");
  console.log("  GET  /tables              - List all tables");
  console.log("  GET  /table/:name/preview - Preview table (10 rows)");
  console.log("  GET  /table/:name/full    - Full table with pagination");
  console.log("  GET  /table/:name/count   - Get row count");
  console.log("  POST /upload              - Upload CSV/Excel/JSON");
  console.log("  POST /ask                 - Multi-table AI query");
  console.log("  POST /download            - Download CSV/Excel");
  console.log("  POST /crud                - CRUD operations");
  console.log("  POST /execute             - Execute raw SQL");
  console.log("  POST /export              - Export (legacy)");
  console.log("=================================");
});

// Export for Vercel serverless
module.exports = app;
