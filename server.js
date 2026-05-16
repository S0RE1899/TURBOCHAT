const { WebSocketServer } = require("ws");
const wss = new WebSocketServer({ port: process.env.PORT || 3000 });
const rooms = {};

wss.on("connection", (ws) => {
  let myRoom = null, myId = null;
  ws.on("message", (raw) => {
    const msg = JSON.parse(raw);
    if (msg.type === "join") {
      myRoom = msg.room; myId = msg.id;
      if (!rooms[myRoom]) rooms[myRoom] = new Map();
      const existing = [...rooms[myRoom].keys()];
      rooms[myRoom].set(myId, ws);
      ws.send(JSON.stringify({ type: "peers", ids: existing }));
      existing.forEach(id => rooms[myRoom].get(id)?.send(JSON.stringify({ type: "joined", id: myId })));
    } else if (msg.type === "leave") {
      cleanup();
    } else if (["offer","answer","ice"].includes(msg.type)) {
      rooms[myRoom]?.get(msg.to)?.send(JSON.stringify({ ...msg, from: myId }));
    }
  });
  function cleanup() {
    if (!myRoom || !myId) return;
    rooms[myRoom]?.delete(myId);
    rooms[myRoom]?.forEach(peer => peer.send(JSON.stringify({ type: "left", id: myId })));
    if (rooms[myRoom]?.size === 0) delete rooms[myRoom];
    myRoom = null; myId = null;
  }
  ws.on("close", cleanup);
});