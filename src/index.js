import { DurableObject } from "cloudflare:workers";

/**
 * DURABLE OBJECT: Manages a single bridge (robot) and its connected browsers
 */
export class BridgeRelay extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
    
    // Map: WebSocket -> metadata { type: 'bridge' | 'browser', bridgeId: string }
    this.sessions = new Map();
    // Store the active bridge WebSocket connection separately for quick access
    this.bridge = null;
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Expected WebSocket connection', { status: 400 });
    }

    // Create a WebSocket pair for the connection
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Accept the WebSocket connection inside the Worker
    server.accept();

    // Track the initial session as pending authentication/identification
    this.sessions.set(server, { type: 'pending' });

    // Listen for incoming messages from this connection
    server.addEventListener('message', async (event) => {
      try {
        const data = JSON.parse(event.data);
        await this.handleMessage(server, data);
      } catch (err) {
        console.error('Message parsing error:', err);
        server.send(JSON.stringify({ type: 'error', message: 'Invalid JSON payload' }));
      }
    });

    // Handle sudden disconnects
    server.addEventListener('close', () => {
      this.handleClose(server);
    });

    server.addEventListener('error', () => {
      this.handleClose(server);
    });

    // Return the client side of the WebSocket pair back to the connector
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleMessage(server, data) {
    const session = this.sessions.get(server);

    // Handle initial client identification handshake
    if (data.type === 'register') {
      const { role, token, bridgeId } = data;

      // Validate the Shared Token securely
      if (token !== this.env.BRIDGE_TOKEN) {
        server.send(JSON.stringify({ type: 'error', message: 'Unauthorized: Invalid Token' }));
        server.close(1008, 'Unauthorized');
        return;
      }

      if (role === 'bridge') {
        this.bridge = server;
        this.sessions.set(server, { type: 'bridge', bridgeId: bridgeId || 'default' });
        console.log(`[DO] Bridge registered: ${bridgeId || 'default'}`);
        server.send(JSON.stringify({ type: 'registered', status: 'ready' }));
        
        // Notify any waiting browsers that the bridge is now online
        this.broadcastToBrowsers({ type: 'bridge_online', bridgeId: bridgeId || 'default' });
      } 
      else if (role === 'browser') {
        this.sessions.set(server, { type: 'browser', bridgeId: bridgeId || 'default' });
        console.log(`[DO] Browser registered for bridge: ${bridgeId || 'default'}`);
        server.send(JSON.stringify({ 
          type: 'registered', 
          bridge_status: this.bridge ? 'online' : 'offline' 
        }));
      }
      return;
    }

    // Route messages if the session is already authenticated
    if (!session || session.type === 'pending') {
      server.send(JSON.stringify({ type: 'error', message: 'Unregistered connection' }));
      return;
    }

    if (session.type === 'bridge') {
      // Broadcast telemetry data from the robot to all browsing listeners
      this.broadcastToBrowsers(data);
    } else if (session.type === 'browser') {
      // Forward commands directly to the robot bridge
      if (this.bridge) {
        try {
          this.bridge.send(JSON.stringify(data));
        } catch (e) {
          server.send(JSON.stringify({ type: 'error', message: 'Failed to deliver payload to bridge' }));
        }
      } else {
        // Changed from 'error' to 'info' type
        server.send(JSON.stringify({ type: 'info', message: 'Bridge is offline' }));
      }
    }
  }

  broadcastToBrowsers(message) {
    const payload = JSON.stringify(message);
    for (const [ws, session] of this.sessions) {
      if (session.type === 'browser') {
        try {
          ws.send(payload);
        } catch (e) {
          // Clean up broken client connections found during looping
          this.handleClose(ws);
        }
      }
    }
  }

  handleClose(server) {
    const session = this.sessions.get(server);
    if (!session) return;

    if (session.type === 'bridge') {
      this.bridge = null;
      console.log(`[DO] Bridge ${session.bridgeId} disconnected`);
      this.broadcastToBrowsers({ type: 'bridge_offline', bridgeId: session.bridgeId });
    } else if (session.type === 'browser') {
      console.log(`[DO] Browser disconnected from bridge ${session.bridgeId}`);
    }

    this.sessions.delete(server);
  }
}

/**
 * WORKER: Main entry point
 */
export default {
  async fetch(request, env) {
    // Route all incoming requests directly into a single central Durable Object namespace instance
    const id = env.BRIDGE_RELAY.idFromName("global_relay_instance");
    const stub = env.BRIDGE_RELAY.get(id);
    return stub.fetch(request);
  },
};