import { describe, test, expect, beforeEach, afterEach, vi } from "vitest";
import { createTestRedisConfig, wait } from "./test-utils";
import { MeshClient } from "@mesh-kit/client";
import { MeshServer } from "@mesh-kit/server";

const REDIS_DB = 8;
const { flushRedis, redisOptions } = createTestRedisConfig(REDIS_DB);

const createTestServer = (port: number) =>
  new MeshServer({
    port,
    redisOptions,
    pingInterval: 1000,
    latencyInterval: 500,
  });

describe("Record Deletion Notifications", () => {
  let server: MeshServer;
  let client: MeshClient;

  beforeEach(async () => {
    await flushRedis();

    server = createTestServer(0);
    server.exposeRecord(/^test:record:.*/);
    await server.ready();

    client = new MeshClient(`ws://localhost:${server.port}`);
  });

  afterEach(async () => {
    await client.close();
    await server.close();
  });

  test("should notify client when subscribed record is deleted", async () => {
    const recordId = "test:record:1";
    const initialData = { id: recordId, name: "Test Record", value: 42 };

    await server.writeRecord(recordId, initialData);
    await client.connect();

    const updates: Array<{ recordId: string; full?: any; patch?: any; version: number; deleted?: boolean }> = [];
    const callback = vi.fn((update) => {
      updates.push(update);
    });

    const subscriptionResult = await client.subscribeRecord(recordId, callback);
    expect(subscriptionResult.success).toBe(true);
    expect(subscriptionResult.record).toEqual(initialData);
    expect(subscriptionResult.version).toBe(1);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(updates[0]).toEqual({
      recordId,
      full: initialData,
      version: 1,
    });

    await server.deleteRecord(recordId);
    await wait(100);

    expect(callback).toHaveBeenCalledTimes(2);
    expect(updates[1]).toEqual({
      recordId,
      deleted: true,
      version: 1,
    });
  });

  test("should not notify client when non-subscribed record is deleted", async () => {
    const recordId = "test:record:2";
    const otherRecordId = "test:record:3";

    await server.writeRecord(recordId, { id: recordId, name: "Test Record 2" });
    await server.writeRecord(otherRecordId, { id: otherRecordId, name: "Test Record 3" });
    await client.connect();

    // only subscribe to the first record
    const callback = vi.fn();
    await client.subscribeRecord(recordId, callback);
    callback.mockClear();

    // delete the other record
    await server.deleteRecord(otherRecordId);
    await wait(100);

    expect(callback).not.toHaveBeenCalled();
  });

  test("should handle deletion of non-existent record gracefully", async () => {
    const recordId = "test:record:nonexistent";

    await expect(server.deleteRecord(recordId)).resolves.not.toThrow();
  });

  test("should clean up client subscription after deletion notification", async () => {
    const recordId = "test:record:cleanup";
    const initialData = { id: recordId, name: "Cleanup Test" };

    await server.writeRecord(recordId, initialData);
    await client.connect();

    const callback = vi.fn();
    await client.subscribeRecord(recordId, callback);
    expect(client["recordSubscriptions"].has(recordId)).toBe(true);

    await server.deleteRecord(recordId);
    await wait(100);

    expect(client["recordSubscriptions"].has(recordId)).toBe(false);
  });

  test("should handle multiple clients subscribed to same record", async () => {
    const recordId = "test:record:multi";
    const initialData = { id: recordId, name: "Multi Client Test" };

    const client2 = new MeshClient(`ws://localhost:${server.port}`);
    await client2.connect();

    try {
      await server.writeRecord(recordId, initialData);
      await client.connect();

      const callback1 = vi.fn();
      const callback2 = vi.fn();

      await client.subscribeRecord(recordId, callback1);
      await client2.subscribeRecord(recordId, callback2);

      callback1.mockClear();
      callback2.mockClear();

      await server.deleteRecord(recordId);
      await wait(100);
      expect(callback1).toHaveBeenCalledWith({
        recordId,
        deleted: true,
        version: 1,
      });
      expect(callback2).toHaveBeenCalledWith({
        recordId,
        deleted: true,
        version: 1,
      });

      expect(client["recordSubscriptions"].has(recordId)).toBe(false);
      expect(client2["recordSubscriptions"].has(recordId)).toBe(false);
    } finally {
      await client2.close();
    }
  });

  test("should work with both patch and full subscription modes", async () => {
    const recordId = "test:record:modes";
    const initialData = { id: recordId, name: "Mode Test", counter: 0 };

    await server.writeRecord(recordId, initialData);
    await client.connect();

    const patchCallback = vi.fn();
    await client.subscribeRecord(recordId, patchCallback, { mode: "patch" });

    const client2 = new MeshClient(`ws://localhost:${server.port}`);
    await client2.connect();

    try {
      const fullCallback = vi.fn();
      await client2.subscribeRecord(recordId, fullCallback, { mode: "full" });

      patchCallback.mockClear();
      fullCallback.mockClear();

      await server.deleteRecord(recordId);
      await wait(100);
      expect(patchCallback).toHaveBeenCalledWith({
        recordId,
        deleted: true,
        version: 1,
      });
      expect(fullCallback).toHaveBeenCalledWith({
        recordId,
        deleted: true,
        version: 1,
      });
    } finally {
      await client2.close();
    }
  });
});

describe("Record Deletion Notifications - Multi-Instance", () => {
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
    await clientA.connect();
    await clientB.connect();
  });

  afterEach(async () => {
    await clientA.close();
    await clientB.close();
    await serverA.close();
    await serverB.close();
  });

  test("should propagate deletion notifications across instances", async () => {
    const recordId = "test:record:cross-instance";
    const initialData = { id: recordId, name: "Cross Instance Test" };

    [serverA, serverB].forEach((server) => {
      server.exposeRecord(/^test:record:.*/);
    });

    await serverA.writeRecord(recordId, initialData);

    const callbackA = vi.fn();
    const callbackB = vi.fn();

    await clientA.subscribeRecord(recordId, callbackA);
    await clientB.subscribeRecord(recordId, callbackB);

    callbackA.mockClear();
    callbackB.mockClear();

    // delete from server B
    await serverB.deleteRecord(recordId);
    await wait(100);
    expect(callbackA).toHaveBeenCalledWith({
      recordId,
      deleted: true,
      version: 1,
    });
    expect(callbackB).toHaveBeenCalledWith({
      recordId,
      deleted: true,
      version: 1,
    });
  });
});
