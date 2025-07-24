import { describe, test, expect, beforeEach, afterEach } from "vitest";
import { MeshServer } from "@mesh-kit/server";
import { MeshClient } from "@mesh-kit/client";
import { Status } from "@mesh-kit/shared";
import { createTestRedisConfig } from "./test-utils";

const REDIS_DB = 2;
const { flushRedis, redisOptions } = createTestRedisConfig(REDIS_DB);

const createTestServer = (port: number) =>
  new MeshServer({
    port,
    redisOptions,
  });

describe("MeshClient", () => {
  let server: MeshServer;
  let client: MeshClient;

  beforeEach(async () => {
    await flushRedis();

    server = createTestServer(0);
    await server.ready();

    client = new MeshClient(`ws://localhost:${server.port}`);
  });

  afterEach(async () => {
    await client.close();
    await server.close();
  });

  test("client has connectionId immediately after connect resolves", async () => {
    await client.connect();

    expect(client.connectionId).toBeDefined();
    expect(typeof client.connectionId).toBe("string");

    const connectionId = client.connectionId as string;
    expect(connectionId.length).toBeGreaterThan(0);
  });

  test("command times out when server doesn't respond", async () => {
    server.exposeCommand("never-responds", async () => new Promise(() => {}));

    await client.connect();

    await expect(client.command("never-responds", "Should time out", 100)).rejects.toThrow(/timed out/);
  }, 2000);

  test("an unknown command should return an error object", async () => {
    await client.connect();

    const result = await client.command("unknown-command", "Should fail");
    expect(result).toEqual({
      code: "ENOTFOUND",
      error: 'Command "unknown-command" not found',
      name: "CommandError",
    });
  });

  test("thrown servers errors are serialized to the client", async () => {
    server.exposeCommand("throws-error", async () => {
      throw new Error("This is a test error");
    });

    await client.connect();

    const result = await client.command("throws-error", "Should fail");
    expect(result).toEqual({
      code: "ESERVER",
      error: "This is a test error",
      name: "Error",
    });
  });

  test("handles large payloads without issue", async () => {
    server.exposeCommand("echo", async (ctx) => ctx.payload);
    await client.connect();

    const largeData = {
      array: Array(1000)
        .fill(0)
        .map((_, i) => `item-${i}`),
      nested: {
        deep: {
          object: {
            with: "lots of data",
          },
        },
      },
    };

    const result = await client.command("echo", largeData, 200);
    expect(result).toEqual(largeData);
  });

  test("client emits 'connect' event on successful connection", async () =>
    new Promise<void>((resolve) => {
      client.on("connect", () => {
        expect(client.status).toBe(Status.ONLINE);
        resolve();
      });

      client.connect();
    }));

  test("client emits 'disconnect' and 'close' events on successful disconnection", async () =>
    new Promise<void>((resolve) => {
      let disconnectEmitted = false;
      let closeEmitted = false;

      const checkBothEvents = () => {
        if (disconnectEmitted && closeEmitted) {
          resolve();
        }
      };

      client.on("disconnect", () => {
        expect(client.status).toBe(Status.OFFLINE);
        disconnectEmitted = true;
        checkBothEvents();
      });

      client.on("close", () => {
        expect(client.status).toBe(Status.OFFLINE);
        closeEmitted = true;
        checkBothEvents();
      });

      client.connect().then(() => {
        client.close();
      });
    }));

  test("client emits specific command event on receiving a message", async () => {
    return new Promise<void>((resolve, reject) => {
      // @ts-ignore
      client.on("hello", (data) => {
        try {
          expect(data).toEqual("world");
          resolve();
        } catch (error) {
          reject(error);
        }
      });

      const timeout = setTimeout(() => {
        reject(new Error("Test timed out waiting for hello event"));
      }, 10000);

      client
        .connect()
        .then(() => {
          server.broadcast("hello", "world");
        })
        .catch(reject);

      return () => clearTimeout(timeout);
    });
  }, 15000);

  test("client receives 'ping' messages", async () => {
    const server = new MeshServer({
      port: 8140,
      pingInterval: 10,
      maxMissedPongs: 10,
      redisOptions,
    });

    await server.ready();
    const client = new MeshClient(`ws://localhost:8140`, {
      pingTimeout: 10,
      maxMissedPings: 10,
    });
    await client.connect();

    return new Promise<void>((resolve) => {
      client.on("ping", () => {
        expect(client.status).toBe(Status.ONLINE);
        resolve();
      });
      client.on("close", () => {
        expect(client.status).toBe(Status.OFFLINE);
      });
    });
  });

  test("client receives 'latency' messages", async () => {
    const server = new MeshServer({
      port: 8141,
      pingInterval: 10,
      latencyInterval: 10,
      maxMissedPongs: 10,
      redisOptions,
    });

    await server.ready();
    const client = new MeshClient(`ws://localhost:8141`, {
      pingTimeout: 10,
      maxMissedPings: 10,
    });
    await client.connect();

    return new Promise<void>((resolve) => {
      client.on("latency", (data) => {
        expect(data).toBeDefined();
        resolve();
      });
      client.on("close", () => {
        expect(client.status).toBe(Status.OFFLINE);
      });
    });
  });

  test("client can get room metadata", async () => {
    const roomName = "test-room";
    const metadata = { key: "value", nested: { data: true } };

    await client.connect();
    await client.joinRoom(roomName);

    await server.roomManager.setMetadata(roomName, metadata);

    const retrievedMetadata = await client.getRoomMetadata(roomName);
    expect(retrievedMetadata).toEqual(metadata);
  });

  test("client can get connection metadata", async () => {
    await client.connect();
    const clientConnection = server.connectionManager.getLocalConnections()[0]!;

    const metadata = { user: "test-user", permissions: ["read", "write"] };
    await server.connectionManager.setMetadata(clientConnection, metadata);

    const retrievedMetadata = await client.getConnectionMetadata(clientConnection.id);
    expect(retrievedMetadata).toEqual(metadata);
  });

  test("client can get their own connection metadata", async () => {
    await client.connect();
    const clientConnection = server.connectionManager.getLocalConnections()[0]!;
    const metadata = { user: "test-user", permissions: ["read", "write"] };
    await server.connectionManager.setMetadata(clientConnection, metadata);
    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual(metadata);
  });

  test("client can set their own connection metadata", async () => {
    await client.connect();
    const metadata = { user: "test-user", status: "online", preferences: { theme: "dark" } };

    const success = await client.setConnectionMetadata(metadata);
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual(metadata);
  });

  test("client can overwrite existing connection metadata", async () => {
    await client.connect();

    const initialMetadata = { user: "initial-user", status: "away" };
    await client.setConnectionMetadata(initialMetadata);

    const newMetadata = { user: "updated-user", status: "online", newField: "value" };
    const success = await client.setConnectionMetadata(newMetadata);
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual(newMetadata);
  });

  test("client can set null metadata", async () => {
    await client.connect();

    const success = await client.setConnectionMetadata(null);
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toBeNull();
  });

  test("client can set empty object metadata", async () => {
    await client.connect();

    const success = await client.setConnectionMetadata({});
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual({});
  });

  test("client can set metadata with replace strategy (default)", async () => {
    await client.connect();

    const initialMetadata = { user: "test-user", status: "online", preferences: { theme: "dark", lang: "en" } };
    await client.setConnectionMetadata(initialMetadata);

    const newMetadata = { user: "updated-user", role: "admin" };
    const success = await client.setConnectionMetadata(newMetadata, { strategy: "replace" });
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual(newMetadata);
  });

  test("client can set metadata with merge strategy", async () => {
    await client.connect();

    const initialMetadata = { user: "test-user", status: "online", preferences: { theme: "dark", lang: "en" } };
    await client.setConnectionMetadata(initialMetadata);

    const updateMetadata = { status: "away", role: "admin" };
    const success = await client.setConnectionMetadata(updateMetadata, { strategy: "merge" });
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual({
      user: "test-user",
      status: "away",
      role: "admin",
      preferences: { theme: "dark", lang: "en" },
    });
  });

  test("client can set metadata with deepMerge strategy", async () => {
    await client.connect();

    const initialMetadata = {
      user: "test-user",
      status: "online",
      preferences: { theme: "dark", lang: "en", notifications: { email: true } },
    };
    await client.setConnectionMetadata(initialMetadata);

    const updateMetadata = {
      status: "away",
      preferences: { theme: "light", notifications: { push: false } },
    };
    const success = await client.setConnectionMetadata(updateMetadata, { strategy: "deepMerge" });
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual({
      user: "test-user",
      status: "away",
      preferences: {
        theme: "light",
        lang: "en",
        notifications: { email: true, push: false },
      },
    });
  });

  test("client merge strategy handles non-object metadata gracefully", async () => {
    await client.connect();

    await client.setConnectionMetadata("initial-string");
    const success = await client.setConnectionMetadata({ user: "test-user" }, { strategy: "merge" });
    expect(success).toBe(true);

    const retrievedMetadata = await client.getConnectionMetadata();
    expect(retrievedMetadata).toEqual({ user: "test-user" });
  });
});
