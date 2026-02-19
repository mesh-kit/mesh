import { describe, test, expect, beforeEach, afterEach } from "vitest";
import { MeshServer } from "@mesh-kit/server";
import { MeshClient } from "@mesh-kit/client";
import { Status } from "@mesh-kit/shared";
import { createTestRedisConfig } from "./test-utils";

const REDIS_DB = 15;
const { flushRedis, redisOptions } = createTestRedisConfig(REDIS_DB);

describe("authenticateConnection", () => {
  let server: MeshServer;

  beforeEach(async () => {
    await flushRedis();
  });

  afterEach(async () => {
    if (server) {
      await server.close();
    }
  });

  test("accepts connection when authenticateConnection returns truthy value", async () => {
    server = new MeshServer({
      port: 0,
      redisOptions,
      authenticateConnection: async (req) => {
        return { userId: 123, role: "admin" };
      },
    });
    await server.ready();

    const client = new MeshClient(`ws://localhost:${server.port}`);
    await client.connect();

    expect(client.status).toBe(Status.ONLINE);

    const connections = server.connectionManager.getLocalConnections();
    expect(connections.length).toBe(1);

    const metadata = await server.connectionManager.getMetadata(connections[0]);
    expect(metadata).toEqual({ userId: 123, role: "admin" });

    await client.close();
  });

  test("rejects connection when authenticateConnection returns null", async () => {
    server = new MeshServer({
      port: 0,
      redisOptions,
      authenticateConnection: async () => {
        return null;
      },
    });
    await server.ready();

    const client = new MeshClient(`ws://localhost:${server.port}`);

    await expect(client.connect()).rejects.toThrow();
    expect(client.status).toBe(Status.OFFLINE);
  });

  test("rejects connection when authenticateConnection throws", async () => {
    server = new MeshServer({
      port: 0,
      redisOptions,
      authenticateConnection: async () => {
        throw new Error("Invalid token");
      },
    });
    await server.ready();

    const client = new MeshClient(`ws://localhost:${server.port}`);

    await expect(client.connect()).rejects.toThrow();
    expect(client.status).toBe(Status.OFFLINE);
  });

  test("rejects with custom status code when error has code property", async () => {
    server = new MeshServer({
      port: 0,
      redisOptions,
      authenticateConnection: async () => {
        throw { code: 403, message: "Forbidden" };
      },
    });
    await server.ready();

    const client = new MeshClient(`ws://localhost:${server.port}`);

    await expect(client.connect()).rejects.toThrow();
    expect(client.status).toBe(Status.OFFLINE);
  });

  test("has access to request headers in authenticateConnection", async () => {
    let capturedHeaders: any = null;

    server = new MeshServer({
      port: 0,
      redisOptions,
      authenticateConnection: async (req) => {
        capturedHeaders = req.headers;
        return { authenticated: true };
      },
    });
    await server.ready();

    const client = new MeshClient(`ws://localhost:${server.port}`);
    await client.connect();

    expect(capturedHeaders).not.toBeNull();
    expect(capturedHeaders.host).toContain(`localhost:${server.port}`);

    await client.close();
  });

  test("metadata from authenticateConnection is accessible in commands", async () => {
    server = new MeshServer({
      port: 0,
      redisOptions,
      authenticateConnection: async () => {
        return { userId: 456, email: "test@example.com" };
      },
    });

    server.exposeCommand("whoami", async (ctx) => {
      const metadata = await server.connectionManager.getMetadata(ctx.connection);
      return metadata;
    });

    await server.ready();

    const client = new MeshClient(`ws://localhost:${server.port}`);
    await client.connect();

    const result = await client.command("whoami", {});
    expect(result).toEqual({ userId: 456, email: "test@example.com" });

    await client.close();
  });

  test("works without authenticateConnection (backward compatibility)", async () => {
    server = new MeshServer({
      port: 0,
      redisOptions,
    });
    await server.ready();

    const client = new MeshClient(`ws://localhost:${server.port}`);
    await client.connect();

    expect(client.status).toBe(Status.ONLINE);

    await client.close();
  });
});
