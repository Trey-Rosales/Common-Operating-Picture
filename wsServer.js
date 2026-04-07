const WebSocket = require('ws');

let wss;

function startWsServer(serverPort = 8080) {
  if (!wss) {
    wss = new WebSocket.Server({ port: serverPort });
    wss.on('connection', (ws) => {
      console.log('WebSocket client connected');
      ws.send(JSON.stringify({ message: 'Welcome to CoT microservice' }));
    });
    console.log(`WebSocket server running on port ${serverPort}`);
  }

  return {
    broadcast: (data) => {
      const msg = JSON.stringify(data);
      wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(msg);
        }
      });
    },
  };
}

module.exports = { startWsServer: startWsServer() };