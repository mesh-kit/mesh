import { MeshServer } from "@mesh-kit/server";
import { MeshClient } from "@mesh-kit/client";
import { describe, test, expect, beforeEach, afterEach } from "vitest";
import { createTestRedisConfig } from "./test-utils";

const REDIS_DB = 12;
const { flushRedis, redisOptions } = createTestRedisConfig(REDIS_DB);

const createTestServer = (port: number) =>
  new MeshServer({
    port,
    redisOptions,
  });

describe("Resubscription after reconnect", () => {
  let server: MeshServer;
  let client: MeshClient;

  beforeEach(async () => {
    await flushRedis();

    server = createTestServer(0);

    // Expose channels and records for testing
    server.exposeChannel("test:channel");
    server.exposeRecord("test:record");
    server.exposeWritableRecord("test:record");

    await server.ready();

    client = new MeshClient(`ws://localhost:${server.port}`);
    await client.connect();
  });

  afterEach(async () => {
    await client.close();
    await server.close();
  });

  test("client resubscribes to all subscriptions after reconnect", async () => {
    const channelMessages: string[] = [];
    const recordUpdates: any[] = [];
    const presenceUpdates: any[] = [];

    await client.subscribeChannel("test:channel", (message) => {
      channelMessages.push(message);
    });

    await client.subscribeRecord("test:record", (update) => {
      recordUpdates.push(update);
    });

    await client.joinRoom("test:room", (update) => {
      presenceUpdates.push(update);
    });

    const forceReconnect = (client as any).forceReconnect.bind(client);
    forceReconnect();

    await new Promise<void>((resolve) => {
      client.once("reconnect", () => {
        // wait for resubscriptions to complete
        setTimeout(resolve, 100);
      });
    });

    // verify channel subscription is restored by sending a message
    await server.writeChannel("test:channel", "Test message after reconnect");

    // verify record subscription is restored by updating the record
    await server.writeRecord("test:record", { value: "updated" });

    // verify room subscription is restored by checking if client is in the room
    const roomMembers = await server.getRoomMembers("test:room");
    const clientInRoom = roomMembers.includes(client.connectionId!);

    // wait for messages to be processed
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    expect(clientInRoom).toBe(true);
    expect(channelMessages).toContain("Test message after reconnect");
    expect(recordUpdates.length).toBeGreaterThan(0);

    // ensure latest record update exists
    const latestRecordUpdate = recordUpdates[recordUpdates.length - 1];
    expect(latestRecordUpdate.full || latestRecordUpdate.recordId).toBeDefined();
  });
});
