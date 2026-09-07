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
    content TEXT,
    room_name TEXT DEFAULT 'general',
    username TEXT DEFAULT 'Anonymous'
  );
`);

try {
  await db.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS room_name TEXT DEFAULT 'general';");
} catch (e) { }

try {
  await db.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS username TEXT DEFAULT 'Anonymous';");
} catch (e) { }

const __dirname = dirname(fileURLToPath(import.meta.url));

app.get("/", (req, res) => {
  res.sendFile(join(__dirname, "index.html"));
});

// ================= SOCKET LOGIC =================
io.on("connection", async (socket) => {
  const serverId = `${os.hostname()} (PID: ${process.pid})`;
  const room = socket.handshake.auth?.room || "general";
  const authUsername = socket.handshake.auth?.username || "Anonymous";

  socket.join(room);
  console.log(`[${serverId}]: User ${authUsername} (${socket.id}) connected to room ${room}`);

  // Let the client know which server they connected to
  socket.emit("server info", serverId);

  // --- RECEIVE MESSAGE ---
  socket.on("chat message", async (...args) => {
    const msg = args[0];
    const clientOffset = args[1];
    let username = authUsername;
    let callback = null;

    if (typeof args[2] === "string") {
      if (args[2].trim()) username = args[2].trim();
      if (typeof args[3] === "function") callback = args[3];
    } else if (typeof args[2] === "function") {
      callback = args[2];
    }

    try {
      const result = await db.query(
        "INSERT INTO messages (content, client_offset, room_name, username) VALUES ($1, $2, $3, $4) RETURNING id",
        [msg, clientOffset, room, username],
      );

      // Emit message to everyone in the room including username
      io.to(room).emit("chat message", msg, result.rows[0].id, serverId, username);

      if (typeof callback === "function") callback({ status: "ok" });
    } catch (error) {
      // POSTGRES UNIQUE VIOLATION ERROR CODE (duplicate client_offset)
      if (error?.code === "23505") {
        if (typeof callback === "function") callback({ status: "duplicate" });
      } else {
        console.error("DB Error:", error);
        if (typeof callback === "function") callback({ status: "error" });
      }
    }
  });

  // --- RECOVERY LOGIC ---
  if (!socket.recovered) {
    try {
      const serverOffset = socket.handshake.auth?.serverOffset || 0;

      const result = await db.query(
        "SELECT id, content, username FROM messages WHERE id > $1 AND room_name = $2 ORDER BY id ASC",
        [serverOffset, room],
      );

      for (const row of result.rows) {
        socket.emit("chat message", row.content, row.id, serverId, row.username || "Anonymous");
      }
    } catch (err) {
      console.error("Recovery error:", err);
    }
  }

  socket.on("disconnect", () => {
    console.log(`[${serverId}]: User ${authUsername} (${socket.id}) disconnected`);
  });
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT} on ${os.hostname()} (PID: ${process.pid})`);
});
