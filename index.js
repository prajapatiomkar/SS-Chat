import express from "express";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Server } from "socket.io";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { availableParallelism } from "node:os";
import cluster from "node:cluster";
import { createAdapter, setupPrimary } from "@socket.io/cluster-adapter";

// ================= PRIMARY PROCESS =================
if (cluster.isPrimary) {
  const numCPUs = availableParallelism();

  console.log(`Primary ${process.pid} is running`);
  console.log(`Starting ${numCPUs} workers...\n`);

  setupPrimary();

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i,
    });
  }

  cluster.on("exit", (worker) => {
    console.log(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
  // ================= WORKER PROCESS =================
  const PORT = process.env.PORT || 3000;

  const app = express();
  const server = createServer(app);

  const io = new Server(server, {
    connectionStateRecovery: {},
    adapter: createAdapter(),
  });

  // --- DB INIT (ONLY IN WORKER) ---
  const db = await open({
    filename: "chat.db",
    driver: sqlite3.Database,
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    console.log(`Worker ${process.pid}: User connected`);

    // --- RECEIVE MESSAGE ---
    socket.on("chat message", async (msg, clientOffset, callback) => {
      try {
        const result = await db.run(
          "INSERT INTO messages (content, client_offset) VALUES (?, ?)",
          msg,
          clientOffset,
        );

        // Emit message with server offset (id)
        io.emit("chat message", msg, result.lastID);

        callback({ status: "ok" });
      } catch (error) {
        // SQLITE_CONSTRAINT (duplicate client_offset)
        if (error?.errno === 19) {
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

        await db.each(
          "SELECT id, content FROM messages WHERE id > ?",
          [serverOffset],
          (_err, row) => {
            socket.emit("chat message", row.content, row.id);
          },
        );
      } catch (err) {
        console.error("Recovery error:", err);
      }
    }

    socket.on("disconnect", () => {
      console.log(`Worker ${process.pid}: User disconnected`);
    });
  });

  server.listen(PORT, () => {
    console.log(`Worker ${process.pid} running at http://localhost:${PORT}`);
  });
}
