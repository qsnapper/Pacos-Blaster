const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const root = __dirname;
const port = Number(process.env.PORT || 4173);
const host = process.env.HOST || "0.0.0.0";
const rooms = new Map();
const types = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
};

const server = http.createServer((request, response) => {
    const urlPath = request.url === "/" ? "/index.html" : request.url.split("?")[0];
    const filePath = path.normalize(path.join(root, decodeURIComponent(urlPath)));

    if (!filePath.startsWith(root)) {
      response.writeHead(403);
      response.end("Forbidden");
      return;
    }

    fs.readFile(filePath, (error, data) => {
      if (error) {
        response.writeHead(404);
        response.end("Not found");
        return;
      }

      response.writeHead(200, { "Content-Type": types[path.extname(filePath)] || "application/octet-stream" });
      response.end(data);
    });
  });

server.on("upgrade", (request, socket) => {
  if (request.url !== "/session") {
    socket.destroy();
    return;
  }

  const key = request.headers["sec-websocket-key"];
  if (!key) {
    socket.destroy();
    return;
  }

  const accept = crypto
    .createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");

  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );

  socket.roomCode = null;
  socket.role = null;
  socket.frameBuffer = Buffer.alloc(0);
  socket.on("data", (chunk) => handleSocketData(socket, chunk));
  socket.on("close", () => leaveRoom(socket));
  socket.on("error", () => leaveRoom(socket));
});

function handleSocketData(socket, chunk) {
  socket.frameBuffer = Buffer.concat([socket.frameBuffer, chunk]);
  const { messages, pings, remaining } = decodeFrames(socket.frameBuffer);
  socket.frameBuffer = remaining;
  pings.forEach((payload) => sendPong(socket, payload));
  messages.forEach((message) => {
    try {
      handleMessage(socket, JSON.parse(message));
    } catch {
      send(socket, { type: "error", message: "Bad message" });
    }
  });
}

function handleMessage(socket, message) {
  if (message.type === "create") {
    const code = makeRoomCode();
    rooms.set(code, { host: socket, guest: null });
    socket.roomCode = code;
    socket.role = "host";
    send(socket, { type: "created", code, role: "host" });
    return;
  }

  if (message.type === "join") {
    const code = String(message.code || "").trim().toUpperCase();
    const room = rooms.get(code);
    if (!room || room.guest) {
      send(socket, { type: "error", message: "Room not available" });
      return;
    }
    room.guest = socket;
    socket.roomCode = code;
    socket.role = "guest";
    send(socket, { type: "joined", code, role: "guest" });
    send(room.host, { type: "peer", role: "guest" });
    return;
  }

  if (!socket.roomCode) return;
  const room = rooms.get(socket.roomCode);
  if (!room) return;
  const target = socket.role === "host" ? room.guest : room.host;
  if (target) send(target, message);
}

function leaveRoom(socket) {
  const code = socket.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  if (!room) return;
  const target = socket.role === "host" ? room.guest : room.host;
  if (target) send(target, { type: "peer-left" });
  rooms.delete(code);
}

function makeRoomCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  do {
    code = Array.from({ length: 5 }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
  } while (rooms.has(code));
  return code;
}

function send(socket, data) {
  if (!socket || socket.destroyed) return;
  const payload = Buffer.from(JSON.stringify(data));
  const header = payload.length < 126 ? Buffer.from([0x81, payload.length]) : Buffer.from([0x81, 126, payload.length >> 8, payload.length & 255]);
  socket.write(Buffer.concat([header, payload]));
}

function sendPong(socket, payload) {
  if (!socket || socket.destroyed) return;
  const header = payload.length < 126
    ? Buffer.from([0x8a, payload.length])
    : Buffer.from([0x8a, 126, payload.length >> 8, payload.length & 255]);
  socket.write(Buffer.concat([header, payload]));
}

function decodeFrames(buffer) {
  const messages = [];
  const pings = [];
  let offset = 0;
  while (offset + 2 <= buffer.length) {
    const start = offset;
    const byte1 = buffer[offset++];
    const byte2 = buffer[offset++];
    const opcode = byte1 & 0x0f;
    let length = byte2 & 0x7f;
    if (length === 126) {
      if (offset + 2 > buffer.length) { offset = start; break; }
      length = buffer.readUInt16BE(offset);
      offset += 2;
    } else if (length === 127) {
      if (offset + 8 > buffer.length) { offset = start; break; }
      length = Number(buffer.readBigUInt64BE(offset));
      offset += 8;
    }
    const masked = (byte2 & 0x80) !== 0;
    if (masked && offset + 4 > buffer.length) { offset = start; break; }
    const mask = masked ? buffer.subarray(offset, offset + 4) : null;
    if (masked) offset += 4;
    if (offset + length > buffer.length) { offset = start; break; }
    const payload = Buffer.from(buffer.subarray(offset, offset + length));
    offset += length;
    if (masked) {
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
    }
    if (opcode === 8) break;
    if (opcode === 9) { pings.push(payload); continue; }
    if (opcode !== 1) continue;
    messages.push(payload.toString("utf8"));
  }
  return { messages, pings, remaining: buffer.subarray(offset) };
}

server.listen(port, host, () => {
  console.log(`Paco's Blaster running at http://127.0.0.1:${port}/`);
  console.log(`Online session WebSocket ready at ws://127.0.0.1:${port}/session`);
});
