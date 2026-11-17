// Generic WebSocket relay: pairs a single upstream connection with many
// downstream clients, forwarding binary frames between them.
// Upstream connects to /agent; downstream clients connect to /stream.
// Frames: [1B type][4B id BE][payload]. Types: 0x01 DATA, 0x02 OPEN, 0x03 CLOSE.
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;
const TYPE_DATA = 0x01;
const TYPE_OPEN = 0x02;
const TYPE_CLOSE = 0x03;

const server = http.createServer((req, res) => {
  if (req.url === "/health") { res.writeHead(200); res.end("ok"); return; }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ws relay running. upstream: /agent, clients: /stream\n");
});

const wss = new WebSocket.Server({ noServer: true });
let agent = null;
let nextId = 1;
const clients = new Map();

function frame(type, id, payload) {
  const buf = Buffer.alloc(5 + (payload ? payload.length : 0));
  buf[0] = type;
  buf.writeUInt32BE(id, 1);
  if (payload) payload.copy(buf, 5);
  return buf;
}

function sendToAgent(buf) {
  if (agent && agent.readyState === WebSocket.OPEN) {
    agent.send(buf);
    return true;
  }
  return false;
}

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://x");
  if (url.pathname === "/agent") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      if (agent) { try { agent.close(); } catch (e) {} }
      agent = ws;
      console.log("agent connected");
      ws.on("message", (data) => {
        const buf = Buffer.from(data);
        if (buf.length < 5) return;
        const type = buf[0];
        const id = buf.readUInt32BE(1);
        const payload = buf.subarray(5);
        const client = clients.get(id);
        if (type === TYPE_DATA && client && client.readyState === WebSocket.OPEN) {
          client.send(Buffer.from(payload));
        } else if (type === TYPE_CLOSE && client) {
          try { client.close(); } catch (e) {}
          clients.delete(id);
        }
      });
      ws.on("close", () => {
        console.log("agent disconnected");
        if (agent === ws) agent = null;
        for (const [id, c] of clients) { try { c.close(); } catch (e) {} }
        clients.clear();
      });
      ws.on("error", () => {});
    });
  } else if (url.pathname === "/stream") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      const id = nextId++;
      clients.set(id, ws);
      console.log("client connected id=" + id);
      if (!sendToAgent(frame(TYPE_OPEN, id))) {
        ws.close(1013, "no agent");
        clients.delete(id);
        return;
      }
      ws.on("message", (data) => {
        const payload = Buffer.from(data);
        if (!sendToAgent(frame(TYPE_DATA, id, payload))) {
          try { ws.close(1013, "no agent"); } catch (e) {}
        }
      });
      const cleanup = () => {
        if (clients.has(id)) {
          clients.delete(id);
          sendToAgent(frame(TYPE_CLOSE, id));
        }
      };
      ws.on("close", cleanup);
      ws.on("error", cleanup);
    });
  } else {
    socket.destroy();
  }
});

server.listen(PORT, () => console.log("relay listening on " + PORT));
