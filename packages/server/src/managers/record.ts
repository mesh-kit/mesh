import type { Redis } from "ioredis";
import jsonpatch, { type Operation } from "fast-json-patch";
import { deepMerge, isObject } from "@mesh-kit/shared";
import type { MeshServer } from "../server";
import { RECORD_KEY_PREFIX, RECORD_VERSION_KEY_PREFIX } from "../utils/constants";

export class RecordManager {
  private redis: Redis;
  private recordUpdateCallbacks: Array<(data: { recordId: string; value: any }) => Promise<void> | void> = [];
  private recordRemovedCallbacks: Array<(data: { recordId: string; value: any }) => Promise<void> | void> = [];
  private server: MeshServer;

  constructor(options: { redis: Redis; server: MeshServer }) {
    this.redis = options.redis;
    this.server = options.server;
  }

  /**
   * Gets the server instance associated with this record manager
   */
  getServer(): MeshServer {
    return this.server;
  }

  /**
   * Gets the Redis instance used by this record manager
   * This is used by the persistence manager to restore records
   */
  getRedis(): Redis {
    return this.redis;
  }

  recordKey(recordId: string): string {
    return `${RECORD_KEY_PREFIX}${recordId}`;
  }

  recordVersionKey(recordId: string): string {
    return `${RECORD_VERSION_KEY_PREFIX}${recordId}`;
  }

  /**
   * Retrieves a record from Redis by its unique identifier. Attempts to parse
   * the stored data as JSON before returning. If the record does not exist,
   * returns null.
   *
   * @param {string} recordId - The unique identifier of the record to retrieve.
   * @returns {Promise<any | null>} A promise that resolves to the parsed record object,
   * or null if the record does not exist.
   * @throws {SyntaxError} If the stored data is not valid JSON and cannot be parsed.
   * @throws {Error} If an error occurs during the Redis operation.
   */
  async getRecord(recordId: string): Promise<any | null> {
    const data = await this.redis.get(this.recordKey(recordId));
    return data ? JSON.parse(data) : null;
  }

  /**
   * Retrieves the version number associated with the specified record ID from Redis.
   * If no version is found, returns 0.
   *
   * @param {string} recordId - The unique identifier for the record whose version is to be retrieved.
   * @returns {Promise<number>} A promise that resolves to the version number of the record. Returns 0 if not found.
   * @throws {Error} If there is an issue communicating with Redis or parsing the version.
   */
  async getVersion(recordId: string): Promise<number> {
    const version = await this.redis.get(this.recordVersionKey(recordId));
    return version ? parseInt(version, 10) : 0;
  }

  /**
   * Retrieves a record and its associated version from Redis.
   * Fetches both the record data and its version by their respective keys.
   *
   * @param {string} recordId - The unique identifier for the record to retrieve.
   * @returns {Promise<{ record: any | null; version: number }>}
   *          A promise that resolves to an object containing the parsed record (or null if not found)
   *          and its version number (0 if version data is not found or invalid).
   * @throws {Error} If there is a Redis error or if JSON parsing fails for the record data.
   */
  async getRecordAndVersion(recordId: string): Promise<{ record: any | null; version: number }> {
    const pipeline = this.redis.pipeline();
    pipeline.get(this.recordKey(recordId));
    pipeline.get(this.recordVersionKey(recordId));
    const results = await pipeline.exec();

    const recordData = results?.[0]?.[1] as string | null;
    const versionData = results?.[1]?.[1] as string | null;

    const record = recordData ? JSON.parse(recordData) : null;
    const version = versionData ? parseInt(versionData, 10) : 0;

    return { record, version };
  }

  /**
   * Publishes an update to a record by computing and applying a JSON Patch,
   * incrementing the version, and persisting the updated value and version in Redis.
   * If there are no changes between the old and new value, returns null.
   *
   * @param {string} recordId - The unique identifier of the record to update.
   * @param {any} newValue - The new value to set for the record, or partial value when using merge strategy.
   * @param {"replace" | "merge" | "deepMerge"} [strategy="replace"] - Update strategy: "replace" (default) replaces the entire record, "merge" merges with existing object properties, "deepMerge" recursively merges nested objects.
   * @returns {Promise<{ patch: Operation[]; version: number; finalValue: any } | null>}
   *          A promise resolving to an object containing the JSON Patch operations, new version number, and final merged value,
   *          or null if there were no changes to publish.
   * @throws {Error} If there is a failure reading or writing to Redis, or during patch computation, the promise will be rejected with the error.
   */
  async publishUpdate(
    recordId: string,
    newValue: any,
    strategy: "replace" | "merge" | "deepMerge" = "replace",
  ): Promise<{ patch: Operation[]; version: number; finalValue: any } | null> {
    const recordKey = this.recordKey(recordId);
    const versionKey = this.recordVersionKey(recordId);

    const { record: oldValue, version: oldVersion } = await this.getRecordAndVersion(recordId);

    let finalValue: any;

    if (strategy === "merge") {
      if (isObject(oldValue) && isObject(newValue)) {
        finalValue = { ...oldValue, ...newValue };
      } else {
        finalValue = newValue;
      }
    } else if (strategy === "deepMerge") {
      if (isObject(oldValue) && isObject(newValue)) {
        finalValue = deepMerge(oldValue, newValue);
      } else {
        finalValue = newValue;
      }
    } else {
      finalValue = newValue;
    }

    const patch = jsonpatch.compare(oldValue ?? {}, finalValue ?? {});

    if (patch.length === 0) {
      return null;
    }

    const newVersion = oldVersion + 1;

    const pipeline = this.redis.pipeline();
    pipeline.set(recordKey, JSON.stringify(finalValue));
    pipeline.set(versionKey, newVersion.toString());
    await pipeline.exec();

    if (this.recordUpdateCallbacks.length > 0) {
      Promise.all(
        this.recordUpdateCallbacks.map(async (callback) => {
          try {
            await callback({ recordId, value: finalValue });
          } catch (error) {
            console.error(`Error in record update callback for ${recordId}:`, error);
          }
        }),
      ).catch((error) => {
        console.error(`Error in record update callbacks for ${recordId}:`, error);
      });
    }

    return { patch, version: newVersion, finalValue };
  }

  /**
   * Deletes a record and its associated version from Redis storage.
   *
   * @param {string} recordId - The unique identifier of the record to be deleted.
   * @returns {Promise<{ version: number }|null>} A promise that resolves to the final version of the deleted record, or null if the record didn't exist.
   * @throws {Error} If an error occurs during the Redis pipeline execution, the promise will be rejected with the error.
   */
  async deleteRecord(recordId: string): Promise<{ version: number } | null> {
    const { record, version } = await this.getRecordAndVersion(recordId);

    if (!record) {
      return null;
    }

    const pipeline = this.redis.pipeline();
    pipeline.del(this.recordKey(recordId));
    pipeline.del(this.recordVersionKey(recordId));
    await pipeline.exec();

    if (this.recordRemovedCallbacks.length > 0) {
      Promise.all(
        this.recordRemovedCallbacks.map(async (callback) => {
          try {
            await callback({ recordId, value: record });
          } catch (error) {
            console.error(`Error in record removed callback for ${recordId}:`, error);
          }
        }),
      ).catch((error) => {
        console.error(`Error in record removed callbacks for ${recordId}:`, error);
      });
    }

    return { version };
  }

  /**
   * Registers a callback function to be called when a record is updated.
   *
   * @param {(data: { recordId: string; value: any }) => Promise<void> | void} callback - The callback function to execute when a record is updated.
   * @returns {() => void} A function that, when called, will unregister the callback.
   */
  onRecordUpdate(callback: (data: { recordId: string; value: any }) => Promise<void> | void): () => void {
    this.recordUpdateCallbacks.push(callback);
    return () => {
      this.recordUpdateCallbacks = this.recordUpdateCallbacks.filter((cb) => cb !== callback);
    };
  }

  /**
   * Registers a callback function to be called when a record is removed.
   *
   * @param {(data: { recordId: string; value: any }) => Promise<void> | void} callback - The callback function to execute when a record is removed.
   * @returns {() => void} A function that, when called, will unregister the callback.
   */
  onRecordRemoved(callback: (data: { recordId: string; value: any }) => Promise<void> | void): () => void {
    this.recordRemovedCallbacks.push(callback);
    return () => {
      this.recordRemovedCallbacks = this.recordRemovedCallbacks.filter((cb) => cb !== callback);
    };
  }
}
