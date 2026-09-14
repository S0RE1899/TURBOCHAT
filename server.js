const { WebSocketServer } = require("ws");
const wss = new WebSocketServer({ port: process.env.PORT || 3000 });
const rooms = {};
const roomPublicVars = {}; // room -> { name: value } — server is the single source of truth

wss.on("connection", (ws) => {
  let myRoom = null, myId = null;
  ws.on("message", (raw) => {
    const msg = JSON.parse(raw);
    if (msg.type === "join") {
      myRoom = msg.room; myId = msg.id;
      if (!rooms[myRoom]) rooms[myRoom] = new Map();
      if (!roomPublicVars[myRoom]) roomPublicVars[myRoom] = {};
      const existing = [...rooms[myRoom].keys()];
      rooms[myRoom].set(myId, ws);
      ws.send(JSON.stringify({ type: "peers", ids: existing }));
      // Catch the newcomer up on the room's current public variables right away
      ws.send(JSON.stringify({ type: "public_vars_snapshot", vars: roomPublicVars[myRoom] }));
      existing.forEach(id => rooms[myRoom].get(id)?.send(JSON.stringify({ type: "joined", id: myId })));
    } else if (msg.type === "leave") {
      cleanup();
    } else if (msg.type === "var_public_create") {
      if (!myRoom) return;
      const vars = roomPublicVars[myRoom] || (roomPublicVars[myRoom] = {});
      // Only initialize if nobody has created it yet — never resets an existing value
      if (!Object.prototype.hasOwnProperty.call(vars, msg.name)) vars[msg.name] = 0;
      rooms[myRoom]?.forEach(peer => peer.send(JSON.stringify({ type: "var_public", name: msg.name, value: vars[msg.name] })));
    } else if (msg.type === "var_public_set") {
      if (!myRoom) return;
      const vars = roomPublicVars[myRoom] || (roomPublicVars[myRoom] = {});
      vars[msg.name] = msg.value;
      rooms[myRoom]?.forEach(peer => peer.send(JSON.stringify({ type: "var_public", name: msg.name, value: msg.value })));
    } else if (msg.type === "var_public_change") {
      if (!myRoom) return;
      const vars = roomPublicVars[myRoom] || (roomPublicVars[myRoom] = {});
      const next = (Number(vars[msg.name]) || 0) + (Number(msg.delta) || 0);
      vars[msg.name] = next;
      rooms[myRoom]?.forEach(peer => peer.send(JSON.stringify({ type: "var_public", name: msg.name, value: next })));
    } else if (["offer","answer","ice"].includes(msg.type)) {
      rooms[myRoom]?.get(msg.to)?.send(JSON.stringify({ ...msg, from: myId }));
    } else if (myRoom) {
      // Generic broadcast: relay any other message type to all other peers in the room
      rooms[myRoom]?.forEach((peer, id) => {
        if (id !== myId) {
          peer.send(JSON.stringify({ ...msg, from: myId }));
        }
      });
    }
  });
  function cleanup() {
    if (!myRoom || !myId) return;
    rooms[myRoom]?.delete(myId);
    rooms[myRoom]?.forEach(peer => peer.send(JSON.stringify({ type: "left", id: myId })));
    if (rooms[myRoom]?.size === 0) { delete rooms[myRoom]; delete roomPublicVars[myRoom]; }
    myRoom = null; myId = null;
  }
  ws.on("close", cleanup);
});
