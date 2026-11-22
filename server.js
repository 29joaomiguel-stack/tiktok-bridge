// Ponte TikTok Live -> sua página
// Uso: TIKTOK_USER=seu_usuario node server.js
import express from "express";
import cors from "cors";
import { WebcastPushConnection } from "tiktok-live-connector";

const TIKTOK_USER = (process.env.TIKTOK_USER || "").replace("@", "");
const PORT = process.env.PORT || 3001;
// Domínios que podem ler o feed (separe por vírgula). "*" libera tudo.
const ORIGINS = process.env.ALLOWED_ORIGINS || "*";

if (!TIKTOK_USER) {
  console.error("Defina TIKTOK_USER (o @ da live, sem o @).");
  process.exit(1);
}

const app = express();
app.use(cors({ origin: ORIGINS === "*" ? true : ORIGINS.split(",") }));

const clients = new Set();
const seen = new Map(); // uniqueId -> { uniqueId, nickname, avatar, firstSeen, lastComment }
const MAX_NAMES = 5000;

function broadcast(event, payload) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  for (const res of clients) res.write(msg);
}

// --- Conexão com a live (com reconexão automática) ---
let connection;
let retryTimer;
let status = "connecting";

function setStatus(s, extra = {}) {
  status = s;
  broadcast("status", { status: s, ...extra });
  console.log("[status]", s, extra.error || "");
}

function connect() {
  clearTimeout(retryTimer);
  connection = new WebcastPushConnection(TIKTOK_USER);

  connection
    .connect()
    .then((state) => {
      setStatus("connected", { roomId: state.roomId });
    })
    .catch((err) => {
      setStatus("offline", { error: String(err.message || err) });
      retryTimer = setTimeout(connect, 15000); // live ainda não começou? tenta de novo
    });

  connection.on("chat", (data) => {
    const user = {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      avatar: data.profilePictureUrl,
    };
    const isNew = !seen.has(user.uniqueId);
    if (seen.size < MAX_NAMES || !isNew) {
      seen.set(user.uniqueId, {
        ...user,
        firstSeen: seen.get(user.uniqueId)?.firstSeen ?? Date.now(),
        lastComment: data.comment,
      });
    }
    broadcast("chat", { ...user, comment: data.comment, isNew, ts: Date.now() });
  });

  connection.on("disconnected", () => {
    setStatus("offline", { error: "desconectado" });
    retryTimer = setTimeout(connect, 5000);
  });

  connection.on("streamEnd", () => {
    setStatus("ended");
  });
}

// --- Rotas ---
app.get("/events", (req, res) => {
  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.flushHeaders();
  res.write(`event: status\ndata: ${JSON.stringify({ status })}\n\n`);
  res.write(`event: snapshot\ndata: ${JSON.stringify([...seen.values()])}\n\n`);
  clients.add(res);
  const ping = setInterval(() => res.write(": ping\n\n"), 25000);
  req.on("close", () => {
    clearInterval(ping);
    clients.delete(res);
  });
});

app.get("/names", (_req, res) => res.json([...seen.values()]));
app.post("/reset", (_req, res) => {
  seen.clear();
  broadcast("snapshot", []);
  res.json({ ok: true });
});
app.get("/health", (_req, res) => res.json({ status, viewers: clients.size, names: seen.size }));

app.listen(PORT, () => {
  console.log(`Ponte rodando em http://localhost:${PORT}  (live: @${TIKTOK_USER})`);
  connect();
});
