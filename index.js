import express from "express";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import { createClient } from "redis";
import pg from "pg";
import os from "node:os";

const PORT = process.env.PORT || 3000;

const app = express();
const server = createServer(app);

// SETUP REDIS ADAPTER FOR MULTI-SERVER PUB/SUB
const pubClient = createClient({
  url: process.env.REDIS_URL || "redis://localhost:6379",
});
const subClient = pubClient.duplicate();

await Promise.all([pubClient.connect(), subClient.connect()]);

const io = new Server(server, {
  connectionStateRecovery: {},
  adapter: createAdapter(pubClient, subClient),
});

// SETUP POSTGRESQL FOR CENTRALIZED DATABASE
const { Pool } = pg;
const db = new Pool({
  connectionString:
    process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/chat",
});

await db.query(`
  CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    client_offset TEXT UNIQUE,
    content TEXT
  );
`);

const __dirname = dirname(fileURLToPath(import.meta.url));

app.get("/", (req, res) => {
  res.sendFile(join(__dirname, "index.html"));
});

// ================= SOCKET LOGIC =================
io.on("connection", async (socket) => {
  const serverId = `${os.hostname()} (PID: ${process.pid})`;
  console.log(`[${serverId}]: User ${socket.id} connected`);

  // Let the client know which server they connected to
  socket.emit("server info", serverId);

  // --- RECEIVE MESSAGE ---
  socket.on("chat message", async (msg, clientOffset, callback) => {
    try {
      const result = await db.query(
        "INSERT INTO messages (content, client_offset) VALUES ($1, $2) RETURNING id",
        [msg, clientOffset],
      );

      // Emit message with server offset (id) and the server that processed it
      io.emit("chat message", msg, result.rows[0].id, serverId);

      callback({ status: "ok" });
    } catch (error) {
      // POSTGRES UNIQUE VIOLATION ERROR CODE (duplicate client_offset)
      if (error?.code === "23505") {
        callback({ status: "duplicate" });
      } else {
        console.error("DB Error:", error);
        callback({ status: "error" });
      }
    }
  });

  // --- RECOVERY LOGIC ---
  if (!socket.recovered) {
    try {
      const serverOffset = socket.handshake.auth?.serverOffset || 0;

      const result = await db.query(
        "SELECT id, content FROM messages WHERE id > $1 ORDER BY id ASC",
        [serverOffset],
      );

      for (const row of result.rows) {
        socket.emit("chat message", row.content, row.id, serverId);
      }
    } catch (err) {
      console.error("Recovery error:", err);
    }
  }

  socket.on("disconnect", () => {
    console.log(`[${serverId}]: User ${socket.id} disconnected`);
  });
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT} on ${os.hostname()} (PID: ${process.pid})`);
});
