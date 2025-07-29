import { describe, test, expect, beforeEach, afterEach } from "vitest";
import { MeshServer } from "@mesh-kit/server";
import { MeshClient } from "@mesh-kit/client";
import { createTestRedisConfig } from "./test-utils";

const REDIS_DB = 13;
const { flushRedis, redisOptions } = createTestRedisConfig(REDIS_DB);

const createTestServer = (port: number) =>
  new MeshServer({
    port,
    redisOptions,
  });

describe("MeshServer", () => {
  let server: MeshServer;
  let clientA: MeshClient;
  let clientB: MeshClient;

  beforeEach(async () => {
    await flushRedis();

    server = createTestServer(0);
    await server.ready();

    clientA = new MeshClient(`ws://localhost:${server.port}`);
    clientB = new MeshClient(`ws://localhost:${server.port}`);
  });

  afterEach(async () => {
    await clientA.close();
    await clientB.close();

    await server.close();
  });

  test("isInRoom", async () => {
    await clientA.connect();
    await clientB.connect();

    await clientA.joinRoom("room1");
    await clientB.joinRoom("room1");
    await clientA.joinRoom("room2");

    const connectionA = server.connectionManager.getLocalConnections()[0]!;
    const connectionB = server.connectionManager.getLocalConnections()[1]!;

    expect(await server.isInRoom("room1", connectionA)).toBe(true);
    expect(await server.isInRoom("room1", connectionB)).toBe(true);
    expect(await server.isInRoom("room2", connectionA)).toBe(true);
    expect(await server.isInRoom("room2", connectionB)).toBe(false);
    expect(await server.isInRoom("room3", connectionA)).toBe(false);
  });

  test("room metadata", async () => {
    const room1 = "meta-room-1";
    const room2 = "meta-room-2";

    const initialMeta1 = { topic: "General", owner: "userA" };
    await server.roomManager.setMetadata(room1, initialMeta1);

    let meta1 = await server.roomManager.getMetadata(room1);
    expect(meta1).toEqual(initialMeta1);

    const updateMeta1 = { topic: "Updated Topic", settings: { max: 10 } };
    await server.roomManager.setMetadata(room1, updateMeta1, { strategy: "merge" });

    meta1 = await server.roomManager.getMetadata(room1);
    expect(meta1).toEqual({ ...initialMeta1, ...updateMeta1 });

    const initialMeta2 = { topic: "Gaming", private: true };
    await server.roomManager.setMetadata(room2, initialMeta2);

    expect(await server.roomManager.getMetadata(room2)).toEqual(initialMeta2);

    expect(await server.roomManager.getMetadata("non-existent-room")).toBeNull();

    const allMeta = await server.roomManager.getAllMetadata();
    expect(allMeta).toEqual(expect.arrayContaining([
      { id: room1, metadata: { ...initialMeta1, ...updateMeta1 } },
      { id: room2, metadata: initialMeta2 },
    ]));

    // clearRoom preserves metadata
    await server.roomManager.clearRoom(room1);
    expect(await server.roomManager.getMetadata(room1)).toEqual({ ...initialMeta1, ...updateMeta1 });

    // deleteRoom removes metadata
    await server.roomManager.deleteRoom(room1);
    expect(await server.roomManager.getMetadata(room1)).toBeNull();
  });

  test("room metadata filtering", async () => {
    const room1 = "filter-room-1";
    const room2 = "filter-room-2";
    const room3 = "filter-room-3";

    await server.roomManager.setMetadata(room1, { type: "public", maxUsers: 10 });
    await server.roomManager.setMetadata(room2, { type: "private", maxUsers: 5 });
    await server.roomManager.setMetadata(room3, { type: "public", maxUsers: 20 });

    // filter by type
    const allRooms = await server.roomManager.getAllMetadata();
    const publicRooms = allRooms.filter((room) => room.metadata.type === "public");

    expect(publicRooms.length).toBe(2);
    expect(publicRooms.map((r) => r.id)).toContain(room1);
    expect(publicRooms.map((r) => r.id)).toContain(room3);
    expect(publicRooms.map((r) => r.id)).not.toContain(room2);

    // filter by maxUsers
    const largeRooms = allRooms.filter((room) => room.metadata.maxUsers > 10);

    expect(largeRooms.length).toBe(1);
    expect(largeRooms.map((r) => r.id)).toContain(room3);
    expect(largeRooms.map((r) => r.id)).not.toContain(room1);
    expect(largeRooms.map((r) => r.id)).not.toContain(room2);
  });

  test("getAllRooms", async () => {
    await clientA.connect();
    await clientB.connect();

    await clientA.joinRoom("room1");
    await clientB.joinRoom("room1");
    await clientA.joinRoom("room2");

    const rooms = await server.getAllRooms();
    expect(rooms).toEqual(expect.arrayContaining(["room1", "room2"]));

    await clientA.leaveRoom("room1");
    const updatedRooms = await server.getAllRooms();
    expect(updatedRooms).toEqual(expect.arrayContaining(["room1", "room2"]));

    await clientB.leaveRoom("room1");
    const finalRooms = await server.getAllRooms();
    expect(finalRooms.length).toBe(1);
    expect(finalRooms).toEqual(expect.arrayContaining(["room2"]));
  });
});
