// src/index.js
// A WebSocket relay for Cloudflare Workers using Durable Objects

// --------------------------------------------
// DURABLE OBJECT: Manages a single bridge and its connected browsers
// --------------------------------------------
export class BridgeRelay {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    // Map: WebSocket -> metadata { type: 'bridge' | 'browser' }
    this.sessions = new Map();
    // Store the bridge WebSocket separately for quick access
    this.bridge = null;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Expected WebSocket', { status: 400 });
    }

    // Create a WebSocket pair
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Accept the WebSocket connection
    server.accept();

    // Store the session
    this.sessions.set(server, { type: 'pending' });

    // Handle messages
    server.addEventListener('message', async (event) => {
      try {
        const data = JSON.parse(event.data);
        await this.handleMessage(server, data);
      } catch (err) {
        console.error('Message error:', err);
        server.send(JSON.stringify({ type: 'error', message: 'Invalid message' }));
      }
    });

    server.addEventListener('close', () => {
      this.handleClose(server);
    });

    server.addEventListener('error', () => {
      this.handleClose(server);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async handleMessage(server, data) {
    const session = this.sessions.get(server);
    if (!session) return;

    // --- Registration (Bridge) ---
    if (data.type === 'register') {
      const { bridge_id, token } = data;
      // Validate token (use env.BRIDGE_TOKEN)
      if (!bridge_id || token !== this.env.BRIDGE_TOKEN) {
        server.send(JSON.stringify({ type: 'error', message: 'Invalid bridge ID or token' }));
        server.close();
        return;
      }

      // If there's already a bridge, disconnect it
      if (this.bridge) {
        this.bridge.send(JSON.stringify({ type: 'error', message: 'New bridge connected' }));
        this.bridge.close();
        this.sessions.delete(this.bridge);
        this.bridge = null;
      }

      // Register this as the bridge
      this.bridge = server;
      this.sessions.set(server, { type: 'bridge', bridgeId: bridge_id });
      server.send(JSON.stringify({ type: 'registered', bridge_id }));
      console.log(`[DO] Bridge ${bridge_id} registered`);
      return;
    }

    // --- Select Bridge (Browser) ---
    if (data.type === 'select_bridge') {
      const { bridge_id } = data;
      if (!this.bridge) {
        server.send(JSON.stringify({ type: 'error', message: 'Bridge not available' }));
        server.close();
        return;
      }
      // Associate browser with this bridge
      this.sessions.set(server, { type: 'browser', bridgeId: bridge_id });
      server.send(JSON.stringify({ type: 'bridge_ready', bridge_id }));
      console.log(`[DO] Browser connected to bridge ${bridge_id}`);
      return;
    }

    // --- Message Routing ---
    const sessionType = session.type;
    const bridgeId = session.bridgeId || (this.bridge ? this.sessions.get(this.bridge)?.bridgeId : null);

    if (!bridgeId) {
      server.send(JSON.stringify({ type: 'error', message: 'No bridge associated' }));
      return;
    }

    if (sessionType === 'bridge') {
      // Bridge -> forward to all browsers
      for (const [ws, info] of this.sessions) {
        if (info.type === 'browser') {
          try { ws.send(JSON.stringify(data)); } catch (e) { /* ignore */ }
        }
      }
    } else if (sessionType === 'browser') {
      // Browser -> forward to bridge
      if (this.bridge) {
        try { this.bridge.send(JSON.stringify(data)); } catch (e) { /* handle */ }
      } else {
        server.send(JSON.stringify({ type: 'error', message: 'Bridge disconnected' }));
      }
    }
  }

  handleClose(server) {
    const session = this.sessions.get(server);
    if (!session) return;

    if (session.type === 'bridge') {
      this.bridge = null;
      // Notify all browsers that bridge is offline
      for (const [ws, info] of this.sessions) {
        if (info.type === 'browser') {
          try { ws.send(JSON.stringify({ type: 'bridge_offline', bridgeId: session.bridgeId })); } catch (e) {}
        }
      }
      console.log(`[DO] Bridge ${session.bridgeId} disconnected`);
    } else if (session.type === 'browser') {
      console.log(`[DO] Browser disconnected from bridge ${session.bridgeId}`);
    }

    this.sessions.delete(server);
  }
}

// --------------------------------------------
// WORKER: Entry point
// --------------------------------------------
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // All requests go to the Durable Object
    // Use a fixed name so all connections share the same DO instance
    const id = env.BRIDGE_RELAY.idFromName('default');
    const stub = env.BRIDGE_RELAY.get(id);

    return stub.fetch(request);
  }
};