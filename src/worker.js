export class ChatRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.sessions = new Map();
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket required", {
        status: 426
      });
    }

    const pair = new WebSocketPair();

    const client = pair[0];
    const server = pair[1];

    server.accept();

    const url = new URL(request.url);

    const roomName =
      url.searchParams.get("room") ||
      "observable-room";

    const clientId =
      crypto.randomUUID();

    const session = {
      id: clientId,
      room: roomName,
      clientData: null,
      socket: server
    };

    this.sessions.set(
      server,
      session
    );

    server.addEventListener(
      "message",
      event => {
        this.handleMessage(
          server,
          event.data
        );
      }
    );

    server.addEventListener(
      "close",
      () => {
        this.removeClient(server);
      }
    );

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }

  handleMessage(socket, raw) {
    let packet;

    try {
      packet = JSON.parse(raw);
    } catch {
      this.send(socket, {
        type: "error",
        message: "Invalid JSON"
      });

      return;
    }

    const client =
      this.sessions.get(socket);

    if (!client) {
      return;
    }

    /*
     * Initial connection
     */

    if (packet.type === "connect") {
      client.clientData =
        packet.data || {};

      this.send(socket, {
        type: "connected",
        clientId: client.id
      });

      this.sendRoomMembers(
        client.room
      );

      return;
    }

    /*
     * Subscribe
     */

    if (packet.type === "subscribe") {

      client.room =
        packet.room ||
        "observable-room";

      this.send(socket, {
        type: "room_open",

        room:
          client.room,

        members:
          this.getMembers(client.room)
      });

      this.broadcast(
        client.room,
        {
          type: "member_join",

          room:
            client.room,

          member:
            this.publicClient(client)
        },
        socket
      );

      return;
    }

    /*
     * Publish message
     */

    if (packet.type === "publish") {

      const room =
        packet.room ||
        client.room;

      this.broadcast(
        room,
        {
          type: "message",

          room,

          message:
            packet.message,

          client:
            this.publicClient(client)
        }
      );

      return;
    }
  }

  publicClient(client) {
    return {
      id: client.id,
      clientData: client.clientData
    };
  }

  getMembers(roomName) {

    const members = [];

    for (
      const client
      of this.sessions.values()
    ) {

      if (
        client.room === roomName
      ) {

        members.push(
          this.publicClient(client)
        );
      }
    }

    return members;
  }

  sendRoomMembers(roomName) {

    const members =
      this.getMembers(roomName);

    for (
      const client
      of this.sessions.values()
    ) {

      if (
        client.room === roomName
      ) {

        this.send(client.socket, {
          type: "members",

          room: roomName,

          members
        });
      }
    }
  }

  broadcast(
    roomName,
    packet,
    except = null
  ) {

    for (
      const client
      of this.sessions.values()
    ) {

      if (
        client.room !== roomName
      ) {
        continue;
      }

      if (
        client.socket === except
      ) {
        continue;
      }

      this.send(
        client.socket,
        packet
      );
    }
  }

  removeClient(socket) {

    const client =
      this.sessions.get(socket);

    if (!client) {
      return;
    }

    this.sessions.delete(socket);

    this.broadcast(
      client.room,
      {
        type: "member_leave",

        room:
          client.room,

        member:
          this.publicClient(client)
      }
    );
  }

  send(socket, packet) {

    if (
      socket.readyState ===
      WebSocket.OPEN
    ) {

      socket.send(
        JSON.stringify(packet)
      );
    }
  }
}


export default {

  async fetch(request, env) {

    const url =
      new URL(request.url);

    /*
     * WebSocket endpoint
     */

    if (
      url.pathname === "/drone"
    ) {

      const room =
        url.searchParams.get(
          "channel"
        ) ||
        "default";

      /*
       * One Durable Object per
       * channel/room.
       */

      const id =
        env.CHAT_ROOM.idFromName(
          room
        );

      const stub =
        env.CHAT_ROOM.get(id);

      /*
       * Forward request to
       * Durable Object.
       */

      return stub.fetch(
        request
      );
    }

    /*
     * Health check
     */

    if (
      url.pathname === "/health"
    ) {

      return Response.json({
        online: true
      });
    }

    /*
     * Website files
     */

    return env.ASSETS.fetch(
      request
    );
  }
};
