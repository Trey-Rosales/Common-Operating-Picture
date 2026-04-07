const express = require('express');
const bodyParser = require('body-parser');
const { parseStringPromise } = require('xml2js');
const { CotStore } = require('./cotStore');
const { startWsServer } = require('./wsServer');

const app = express();
const port = 3000;

// Middleware
app.use(bodyParser.text({ type: 'application/xml' }));

// Initialize CoT store
const cotStore = new CotStore();

// API endpoint to receive CoT data
app.post('/cot', async (req, res) => {
  try {
    const cotXml = req.body;
    const cotJson = await parseStringPromise(cotXml);
    
    const delta = cotStore.update(cotJson);
    
    // Broadcast delta to WebSocket clients
    startWsServer.broadcast(delta);
    
    res.status(200).json({ status: 'ok', delta });
  } catch (err) {
    console.error(err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});

// API endpoint to get current CoT state
app.get('/cot', (req, res) => {
  res.json(cotStore.getAll());
});

app.listen(3000, '0.0.0.0', () => {
  console.log(`CoT microservice running on port ${port}`);
});