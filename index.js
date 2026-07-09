const WebSocket = require('ws');
const config = require('./config');

const wss = new WebSocket.Server({ port: config.PORT });

// Store active bridge connections by bridge ID
const bridges = new Map();          // bridgeId -> WebSocket
// Store browser connections associated to a bridge
const bridgeToBrowsers = new Map(); // bridgeId -> Set(WebSocket)
// Reverse map for quick cleanup
const browserToBridge = new Map();  // WebSocket -> bridgeId

console.log(`[Relay] Listening on port ${config.PORT}`);

function sendToBridge(bridgeId, message) {
  const bridgeWs = bridges.get(bridgeId);
  if (bridgeWs && bridgeWs.readyState === WebSocket.OPEN) {
    bridgeWs.send(message);
    return true;
  }
  return false;
}

function forwardToBrowsers(bridgeId, message) {
  const browsersSet = bridgeToBrowsers.get(bridgeId);
  if (!browsersSet) return;
  for (const browser of browsersSet) {
    if (browser.readyState === WebSocket.OPEN) {
      browser.send(message);
    }
  }
}

function removeBridge(bridgeId) {
  const bridgeWs = bridges.get(bridgeId);
  if (bridgeWs) {
    bridgeWs.close();
    bridges.delete(bridgeId);
  }
  // Notify associated browsers that bridge is gone
  const browsersSet = bridgeToBrowsers.get(bridgeId);
  if (browsersSet) {
    for (const browser of browsersSet) {
      browser.send(JSON.stringify({ type: 'bridge_offline', bridgeId }));
      browserToBridge.delete(browser);
    }
    bridgeToBrowsers.delete(bridgeId);
  }
}

function removeBrowser(browser) {
  const bridgeId = browserToBridge.get(browser);
  if (bridgeId) {
    const browsersSet = bridgeToBrowsers.get(bridgeId);
    if (browsersSet) {
      browsersSet.delete(browser);
      if (browsersSet.size === 0) {
        bridgeToBrowsers.delete(bridgeId);
      }
    }
    browserToBridge.delete(browser);
  }
}

wss.on('connection', (ws, req) => {
  // Determine if this is a bridge or browser connection.
  // We'll use the first message to identify.
  let identified = false;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (!identified) {
        // First message must be registration (bridge) or select_bridge (browser)
        if (msg.type === 'register') {
          // Bridge registration
          const { bridge_id, token } = msg;
          if (!bridge_id || token !== config.BRIDGE_TOKEN) {
            ws.send(JSON.stringify({ type: 'error', message: 'Invalid bridge ID or token' }));
            ws.close();
            return;
          }
          // If a bridge with same ID already exists, disconnect old one
          if (bridges.has(bridge_id)) {
            console.log(`[Relay] Bridge ${bridge_id} reconnecting, closing old.`);
            bridges.get(bridge_id).close();
            removeBridge(bridge_id);
          }
          bridges.set(bridge_id, ws);
          identified = true;
          ws.bridgeId = bridge_id;
          console.log(`[Relay] Bridge ${bridge_id} registered.`);
          ws.send(JSON.stringify({ type: 'registered', bridge_id }));
          return;
        }

        if (msg.type === 'select_bridge') {
          // Browser selects a bridge
          const { bridge_id } = msg;
          if (!bridge_id || !bridges.has(bridge_id)) {
            ws.send(JSON.stringify({ type: 'error', message: 'Bridge not available' }));
            return;
          }
          // Associate browser with bridge
          if (!bridgeToBrowsers.has(bridge_id)) {
            bridgeToBrowsers.set(bridge_id, new Set());
          }
          bridgeToBrowsers.get(bridge_id).add(ws);
          browserToBridge.set(ws, bridge_id);
          identified = true;
          console.log(`[Relay] Browser associated with bridge ${bridge_id}`);
          ws.send(JSON.stringify({ type: 'bridge_ready', bridge_id }));
          return;
        }

        // If not identified and not register/select, reject
        ws.send(JSON.stringify({ type: 'error', message: 'First message must be register or select_bridge' }));
        ws.close();
        return;
      }

      // Identified connections: forward messages
      const bridgeId = ws.bridgeId || browserToBridge.get(ws);
      if (!bridgeId) {
        // Should not happen
        return;
      }

      if (bridges.has(bridgeId) && bridges.get(bridgeId) === ws) {
        // Message from bridge -> forward to all browsers of this bridge
        forwardToBrowsers(bridgeId, data.toString());
      } else {
        // Message from browser -> forward to bridge
        sendToBridge(bridgeId, data.toString());
      }
    } catch (e) {
      console.error('[Relay] Error processing message:', e);
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
    }
  });

  ws.on('close', () => {
    // Determine if it's a bridge or browser
    const bridgeId = ws.bridgeId;
    if (bridgeId) {
      console.log(`[Relay] Bridge ${bridgeId} disconnected`);
      removeBridge(bridgeId);
    } else {
      removeBrowser(ws);
      console.log('[Relay] Browser disconnected');
    }
  });

  ws.on('error', (err) => {
    console.error('[Relay] WebSocket error:', err);
  });
});

console.log('[Relay] Server running. Use wss://<your-domain>:' + config.PORT + ' (or ws:// locally)');