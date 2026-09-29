(function (window) {
  "use strict";

  /*
   * Self-hosted ScaleDrone-compatible client
   *
   * Existing application code can continue using:
   *
   *   new ScaleDrone(...)
   *   drone.on(...)
   *   drone.subscribe(...)
   *   drone.publish(...)
   *
   * No Scaledrone account is required.
   */

  class EventEmitter {
    constructor() {
      this.events = {};
    }

    on(event, callback) {
      if (!this.events[event]) {
        this.events[event] = [];
      }

      this.events[event].push(callback);

      return this;
    }

    off(event, callback) {
      if (!this.events[event]) {
        return this;
      }

      this.events[event] =
        this.events[event].filter(fn => fn !== callback);

      return this;
    }

    emit(event, ...args) {
      const listeners = this.events[event] || [];

      listeners.forEach(callback => {
        try {
          callback(...args);
        } catch (error) {
          console.error(error);
        }
      });

      return this;
    }
  }


  class Room extends EventEmitter {

    constructor(drone, name) {
      super();

      this.drone = drone;
      this.name = name;

      this.members = [];
    }

    publish(message) {
      this.drone.publish({
        room: this.name,
        message: message
      });
    }
  }


  class ScaleDrone extends EventEmitter {

    constructor(channelId, options = {}) {
      super();

      this.channelId = channelId;

      this.clientData =
        options.data || {};

      this.clientId = null;

      this.socket = null;

      this.rooms = {};

      this.connected = false;

      this.reconnectTimer = null;

      /*
       * Your WebSocket server.
       *
       * If the chat page is served over HTTPS,
       * the connection automatically uses WSS.
       */

      const protocol =
        window.location.protocol === "https:"
          ? "wss:"
          : "ws:";

      this.server =
        options.server ||
        protocol +
        "//" +
        window.location.host +
        "/drone";
    }


    connect() {

      if (this.socket) {
        try {
          this.socket.close();
        } catch (_) {}
      }

      console.log(
        "Connecting to self-hosted messaging server..."
      );

      this.socket =
        new WebSocket(
          this.server +
          "?channel=" +
          encodeURIComponent(this.channelId)
        );


      this.socket.addEventListener(
        "open",
        () => {

          this.socket.send(
            JSON.stringify({
              type: "connect",

              channel: this.channelId,

              data: this.clientData
            })
          );
        }
      );


      this.socket.addEventListener(
        "message",
        event => {

          let packet;

          try {
            packet =
              JSON.parse(event.data);
          } catch (error) {

            console.error(
              "Invalid server message:",
              event.data
            );

            return;
          }

          this.handlePacket(packet);
        }
      );


      this.socket.addEventListener(
        "error",
        error => {
          this.emit("error", error);
        }
      );


      this.socket.addEventListener(
        "close",
        event => {

          this.connected = false;

          this.emit("close", event);
        }
      );
    }


    subscribe(roomName) {

      if (!this.rooms[roomName]) {

        this.rooms[roomName] =
          new Room(this, roomName);

        if (this.connected) {

          this.send({
            type: "subscribe",
            room: roomName
          });
        }
      }

      return this.rooms[roomName];
    }


    publish(options) {

      if (!options) {
        return;
      }

      this.send({
        type: "publish",

        room: options.room,

        message: options.message
      });
    }


    send(packet) {

      if (
        !this.socket ||
        this.socket.readyState !== WebSocket.OPEN
      ) {

        console.warn(
          "Message server is not connected."
        );

        return;
      }

      this.socket.send(
        JSON.stringify(packet)
      );
    }


    handlePacket(packet) {

      switch (packet.type) {

        /*
         * Server accepted the connection.
         */

        case "connected":

          this.connected = true;

          this.clientId =
            packet.clientId;

          this.emit(
            "open",
            null
          );

          /*
           * Re-subscribe to rooms.
           */

          Object.keys(this.rooms)
            .forEach(roomName => {

              this.send({
                type: "subscribe",
                room: roomName
              });

            });

          break;


        /*
         * Room successfully opened.
         */

        case "room_open": {

          const room =
            this.rooms[packet.room];

          if (!room) {
            break;
          }

          room.members =
            packet.members || [];

          room.emit(
            "open",
            null
          );

          break;
        }


        /*
         * Complete member list.
         */

        case "members": {

          const room =
            this.rooms[packet.room];

          if (!room) {
            break;
          }

          room.members =
            packet.members || [];

          room.emit(
            "members",
            room.members
          );

          break;
        }


        /*
         * Somebody joined.
         */

        case "member_join": {

          const room =
            this.rooms[packet.room];

          if (!room) {
            break;
          }

          const member =
            packet.member;

          room.members.push(member);

          room.emit(
            "member_join",
            member
          );

          break;
        }


        /*
         * Somebody left.
         */

        case "member_leave": {

          const room =
            this.rooms[packet.room];

          if (!room) {
            break;
          }

          const member =
            packet.member;

          room.members =
            room.members.filter(
              existing =>
                existing.id !== member.id
            );

          room.emit(
            "member_leave",
            member
          );

          break;
        }


        /*
         * Chat message.
         */

        case "message": {

          const room =
            this.rooms[packet.room];

          if (!room) {
            break;
          }

          room.emit(
            "data",

            packet.message,

            packet.client || null
          );

          break;
        }


        /*
         * Server error.
         */

        case "error":

          this.emit(
            "error",

            new Error(
              packet.message ||
              "Messaging server error"
            )
          );

          break;
      }
    }
  }


  /*
   * IMPORTANT:
   *
   * Your existing code says:
   *
   *     new ScaleDrone(...)
   *
   * so expose exactly that global name.
   */

  window.ScaleDrone =
    ScaleDrone;

})(window);
