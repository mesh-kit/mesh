import { MeshServer } from "@mesh-kit/server";
import { describe, test, expect, beforeEach, afterEach, vi } from "vitest";
import { createTestRedisConfig, wait } from "./test-utils";
import { MeshClient } from "@mesh-kit/client";

const REDIS_DB = 11;
const { flushRedis, redisOptions } = createTestRedisConfig(REDIS_DB);

const createTestServer = (port: number) =>
  new MeshServer({
    port,
    redisOptions,
    pingInterval: 1000,
    latencyInterval: 500,
  });

describe("Record Update and Remove Hooks", () => {
  let server: MeshServer;
  let client: MeshClient;

  beforeEach(async () => {
    await flushRedis();

    server = createTestServer(0);
    server.exposeRecord(/^test:record:.*/);
    server.exposeWritableRecord(/^writable:record:.*/);
    await server.ready();

    client = new MeshClient(`ws://localhost:${server.port}`);
    await client.connect();
  });

  afterEach(async () => {
    await client.close();
    await server.close();
  });

  test("onRecordUpdate callback is triggered when a record is updated", async () => {
    const recordId = "test:record:update";
    const initialData = { count: 0, name: "initial" };
    const updatedData = { count: 1, name: "updated" };

    const updateCallback = vi.fn();
    const unregister = server.onRecordUpdate(updateCallback);

    await server.writeRecord(recordId, initialData);
    await wait(50);

    expect(updateCallback).toHaveBeenCalledTimes(1);
    expect(updateCallback).toHaveBeenCalledWith({
      recordId,
      value: initialData,
    });

    await server.writeRecord(recordId, updatedData);
    await wait(50);

    expect(updateCallback).toHaveBeenCalledTimes(2);
    expect(updateCallback).toHaveBeenCalledWith({
      recordId,
      value: updatedData,
    });

    unregister();

    await server.writeRecord(recordId, { count: 2 });
    await wait(50);

    expect(updateCallback).toHaveBeenCalledTimes(2);
  });

  test("onRecordUpdate callback is triggered when a client updates a record", async () => {
    const recordId = "writable:record:client-update";
    const data = { message: "from client" };

    const updateCallback = vi.fn();
    server.onRecordUpdate(updateCallback);

    const success = await client.writeRecord(recordId, data);
    expect(success).toBe(true);

    await wait(100);

    expect(updateCallback).toHaveBeenCalledTimes(1);
    expect(updateCallback).toHaveBeenCalledWith({
      recordId,
      value: data,
    });
  });

  test("onRecordRemoved callback is triggered when a record is deleted", async () => {
    const recordId = "test:record:delete";
    const initialData = { count: 0, name: "to be deleted" };

    await server.writeRecord(recordId, initialData);
    await wait(50);

    const removeCallback = vi.fn();
    const unregister = server.onRecordRemoved(removeCallback);

    await server.recordManager.deleteRecord(recordId);
    await wait(50);

    expect(removeCallback).toHaveBeenCalledTimes(1);
    expect(removeCallback).toHaveBeenCalledWith({
      recordId,
      value: initialData,
    });

    unregister();

    const anotherRecordId = "test:record:another-delete";
    await server.writeRecord(anotherRecordId, { test: true });
    await wait(50);

    await server.recordManager.deleteRecord(anotherRecordId);
    await wait(50);

    expect(removeCallback).toHaveBeenCalledTimes(1);
  });

  test("multiple callbacks can be registered for record updates", async () => {
    const recordId = "test:record:multiple-callbacks";
    const data = { value: "test" };

    const callback1 = vi.fn();
    const callback2 = vi.fn();

    server.onRecordUpdate(callback1);
    server.onRecordUpdate(callback2);

    await server.writeRecord(recordId, data);
    await wait(50);

    expect(callback1).toHaveBeenCalledTimes(1);
    expect(callback1).toHaveBeenCalledWith({
      recordId,
      value: data,
    });

    expect(callback2).toHaveBeenCalledTimes(1);
    expect(callback2).toHaveBeenCalledWith({
      recordId,
      value: data,
    });
  });

  test("multiple callbacks can be registered for record removals", async () => {
    const recordId = "test:record:multiple-remove-callbacks";
    const data = { value: "to be removed" };

    await server.writeRecord(recordId, data);
    await wait(50);

    const callback1 = vi.fn();
    const callback2 = vi.fn();

    server.onRecordRemoved(callback1);
    server.onRecordRemoved(callback2);

    await server.recordManager.deleteRecord(recordId);
    await wait(50);

    expect(callback1).toHaveBeenCalledTimes(1);
    expect(callback1).toHaveBeenCalledWith({
      recordId,
      value: data,
    });

    expect(callback2).toHaveBeenCalledTimes(1);
    expect(callback2).toHaveBeenCalledWith({
      recordId,
      value: data,
    });
  });

  test("error in one callback doesn't prevent other callbacks from executing", async () => {
    const recordId = "test:record:error-handling";
    const data = { value: "test" };

    const originalConsoleError = console.error;
    console.error = vi.fn();

    const errorCallback = vi.fn().mockImplementation(() => {
      throw new Error("Callback error");
    });

    const successCallback = vi.fn();

    server.onRecordUpdate(errorCallback);
    server.onRecordUpdate(successCallback);

    await server.writeRecord(recordId, data);
    await wait(50);

    expect(errorCallback).toHaveBeenCalledTimes(1);
    expect(successCallback).toHaveBeenCalledTimes(1);
    expect(console.error).toHaveBeenCalled();

    console.error = originalConsoleError;
  });

  test("async callbacks are properly awaited", async () => {
    const recordId = "test:record:async";
    const data = { value: "async test" };

    let asyncOperationCompleted = false;

    const asyncCallback = vi.fn().mockImplementation(async () => {
      await wait(50);
      asyncOperationCompleted = true;
    });

    server.onRecordUpdate(asyncCallback);

    await server.writeRecord(recordId, data);

    expect(asyncOperationCompleted).toBe(false);

    await wait(100);

    expect(asyncOperationCompleted).toBe(true);
    expect(asyncCallback).toHaveBeenCalledTimes(1);
  });
});
