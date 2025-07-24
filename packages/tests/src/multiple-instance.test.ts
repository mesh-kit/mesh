import { describe, test, expect, beforeEach, afterEach } from "vitest";
import { createTestRedisConfig } from "./test-utils";
import { MeshServer } from "@mesh-kit/server";
import { MeshClient } from "@mesh-kit/client";

const REDIS_DB = 4;
const { flushRedis, redisOptions } = createTestRedisConfig(REDIS_DB);

const createTestServer = (port: number) =>
  new MeshServer({
    port,
    redisOptions,
  });

describe.sequential("Multiple instances", () => {
  let serverA: MeshServer;
  let serverB: MeshServer;
  let clientA: MeshClient;
  let clientB: MeshClient;

  beforeEach(async () => {
    await flushRedis();

    serverA = createTestServer(0);
    serverB = createTestServer(0);
    await serverA.ready();
    await serverB.ready();

    clientA = new MeshClient(`ws://localhost:${serverA.port}`);
    clientB = new MeshClient(`ws://localhost:${serverB.port}`);
  });

  afterEach(async () => {
    await clientA.close();
    await clientB.close();

    await serverA.close();
    await serverB.close();
  });

  test("broadcast should work across instances", async () => {
    serverA.exposeCommand("broadcast", async () => {
      await serverA.broadcast("hello", "Hello!");
    });

    await clientA.connect();
    await clientB.connect();

    let receivedA = false;
    let receivedB = false;

    // @ts-ignore
    clientA.on("hello", (data) => {
      if (data === "Hello!") receivedA = true;
    });

    // @ts-ignore
    clientB.on("hello", (data) => {
      if (data === "Hello!") receivedB = true;
    });

    await clientA.command("broadcast", {});

    // wait for both events, or timeout
    await new Promise<void>((resolve) => {
      const interval = setInterval(() => {
        if (!(receivedA && receivedB)) return;
        clearTimeout(timeout);
        clearInterval(interval);
        resolve();
      }, 10);

      const timeout = setTimeout(() => {
        clearInterval(interval);
        resolve();
      }, 10000);
    });

    expect(receivedA).toBe(true);
    expect(receivedB).toBe(true);
  }, 10000);

  test("broadcastRoom should work across instances", async () => {
    [serverA, serverB].forEach((server) =>
      server.exposeCommand("join-room", async (ctx) => {
        await server.addToRoom(ctx.payload.room, ctx.connection);
        return { joined: true };
      }),
    );

    serverA.exposeCommand("broadcast-room", async (ctx) => {
      await serverA.broadcastRoom(ctx.payload.room, "room-message", ctx.payload.message);
      return { sent: true };
    });

    await clientA.connect();
    await clientB.connect();

    let receivedA = false;
    let receivedB = false;

    // @ts-ignore
    clientA.on("room-message", (data) => {
      if (data === "hello") receivedA = true;
    });

    // @ts-ignore
    clientB.on("room-message", (data) => {
      if (data === "hello") receivedB = true;
    });

    await clientA.command("join-room", { room: "test-room" });
    await clientB.command("join-room", { room: "test-room" });

    await clientA.command("broadcast-room", {
      room: "test-room",
      message: "hello",
    });

    // wait for both events, or timeout
    await new Promise<void>((resolve) => {
      const interval = setInterval(() => {
        if (!(receivedA && receivedB)) return;
        clearTimeout(timeout);
        clearInterval(interval);
        resolve();
      }, 10);

      const timeout = setTimeout(() => {
        clearInterval(interval);
        resolve();
      }, 10000);
    });

    expect(receivedA).toBe(true);
    expect(receivedB).toBe(true);
  }, 10000);
});
