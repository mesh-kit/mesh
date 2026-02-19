import { EventEmitter } from "events";
import { v4 as uuidv4 } from "uuid";
import { serverLogger } from "@mesh-kit/shared";
import type { PersistenceAdapter, PersistedMessage, ChannelPersistenceOptions, RecordPersistenceConfig, PersistedRecord, CustomPersistedRecord } from "../persistence/types";
import type { SQLitePersistenceAdapter as SQLiteType } from "../persistence/sqlite-adapter";
import type { PostgreSQLPersistenceAdapter as PostgresType } from "../persistence/postgres-adapter";
import { MessageStream } from "../persistence/message-stream";
import { RecordManager } from "./record";
import { convertToSqlPattern } from "../utils/pattern-conversion";

interface ChannelPatternConfig {
  pattern: string | RegExp;
  options: Required<ChannelPersistenceOptions>;
}

interface RecordPatternConfig {
  pattern: string | RegExp;
  adapter?: { adapter: PersistenceAdapter; restorePattern: string };
  hooks?: { persist: (records: CustomPersistedRecord[]) => Promise<void>; restore: () => Promise<CustomPersistedRecord[]> };
  flushInterval: number;
  maxBufferSize: number;
}

type ResolvedAdapterConfig = { adapter: PersistenceAdapter; restorePattern: string };

export class PersistenceManager extends EventEmitter {
  private defaultAdapter: PersistenceAdapter;
  private channelPatterns: ChannelPatternConfig[] = [];
  private recordPatterns: RecordPatternConfig[] = [];
  private messageBuffer: Map<string, PersistedMessage[]> = new Map();
  private recordBuffer: Map<string, PersistedRecord> = new Map();
  private flushTimers: Map<string, NodeJS.Timeout> = new Map();
  private recordFlushTimer: NodeJS.Timeout | null = null;
  private isShuttingDown = false;
  private initialized = false;
  private recordManager: RecordManager | null = null;
  private pendingRecordUpdates: Array<{ recordId: string; value: any; version: number }> = [];

  private messageStream: MessageStream;

  constructor(options: { defaultAdapterOptions?: any; adapterType?: "sqlite" | "postgres" }) {
    super();

    const { defaultAdapterOptions = {}, adapterType = "sqlite" } = options;

    if (adapterType === "postgres") {
      const { PostgreSQLPersistenceAdapter } = require("../persistence/postgres-adapter") as { PostgreSQLPersistenceAdapter: typeof PostgresType };
      this.defaultAdapter = new PostgreSQLPersistenceAdapter(defaultAdapterOptions);
    } else {
      const { SQLitePersistenceAdapter } = require("../persistence/sqlite-adapter") as { SQLitePersistenceAdapter: typeof SQLiteType };
      this.defaultAdapter = new SQLitePersistenceAdapter(defaultAdapterOptions);
    }

    this.messageStream = MessageStream.getInstance();
  }

  /**
   * Sets the record manager reference for record restoration
   * @param recordManager The record manager instance
   */
  setRecordManager(recordManager: RecordManager): void {
    this.recordManager = recordManager;
  }

  /**
   * Waits until the persistence manager is fully ready and initialized.
   *
   * @returns {Promise<void>} A promise that resolves when persistence is ready.
   */
  async ready(): Promise<void> {
    if (this.initialized) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.once("initialized", resolve);
    });
  }

  /**
   * Processes any record updates that were buffered during initialization
   */
  private async processPendingRecordUpdates(): Promise<void> {
    if (this.pendingRecordUpdates.length === 0) return;

    serverLogger.info(`Processing ${this.pendingRecordUpdates.length} pending record updates`);

    const updates = [...this.pendingRecordUpdates];
    this.pendingRecordUpdates = [];

    for (const { recordId, value, version } of updates) {
      this.handleRecordUpdate(recordId, value, version);
    }
  }

  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      await this.defaultAdapter.initialize();

      this.messageStream.subscribeToMessages(this.handleStreamMessage.bind(this));

      this.initialized = true;

      // process any pending record updates that occurred before initialization
      await this.processPendingRecordUpdates();

      this.emit("initialized");
    } catch (err) {
      serverLogger.error("Failed to initialize persistence manager:", err);
      throw err;
    }
  }

  /**
   * Restores persisted records from storage into Redis on startup
   */
  async restorePersistedRecords(): Promise<void> {
    if (!this.recordManager) {
      serverLogger.warn("Cannot restore persisted records: record manager not available");
      return;
    }

    const redis = this.recordManager.getRedis();
    if (!redis) {
      serverLogger.warn("Cannot restore records: Redis not available");
      return;
    }

    try {
      serverLogger.info("Restoring persisted records...");

      if (this.recordPatterns.length === 0) {
        serverLogger.info("No record patterns to restore");
        return;
      }

      serverLogger.info(`Found ${this.recordPatterns.length} record patterns to restore`);
      for (const config of this.recordPatterns) {
        serverLogger.info(`Config keys: ${Object.keys(config).join(", ")}`);
        serverLogger.info(`Config.hooks: ${typeof config.hooks}, Config.adapter: ${typeof config.adapter}`);
        serverLogger.info(`Config.pattern: ${config.pattern}`);
        const { adapter, hooks } = config;
        const patternLabel = hooks ? "(custom hooks)" : adapter?.restorePattern;

        try {
          let records: Array<{ recordId: string; value: unknown; version: number }> = [];

          if (hooks) {
            records = await hooks.restore();
          } else if (adapter) {
            const adapterRecords = adapter.adapter.getRecords
              ? await adapter.adapter.getRecords(adapter.restorePattern)
              : [];
            records = adapterRecords.map((r) => ({
              recordId: r.recordId,
              value: typeof r.value === "string" ? JSON.parse(r.value) : r.value,
              version: r.version,
            }));
          }

          if (records.length > 0) {
            serverLogger.info(`Restoring ${records.length} records for pattern ${patternLabel}`);

            for (const record of records) {
              try {
                const { recordId, value, version } = record;

                const recordKey = this.recordManager.recordKey(recordId);
                const versionKey = this.recordManager.recordVersionKey(recordId);

                const pipeline = redis.pipeline();
                pipeline.set(recordKey, JSON.stringify(value));
                pipeline.set(versionKey, version.toString());
                await pipeline.exec();

                serverLogger.debug(`Restored record ${recordId} (version ${version})`);
              } catch (parseErr) {
                serverLogger.error(`Failed to restore record ${record.recordId}: ${parseErr}`);
              }
            }
          } else {
            serverLogger.debug(`No records found for pattern ${patternLabel}`);
          }
        } catch (patternErr) {
          serverLogger.error(`Error restoring records for pattern ${patternLabel}: ${patternErr}`);
        }
      }

      serverLogger.info("Finished restoring persisted records");
    } catch (err) {
      serverLogger.error("Failed to restore persisted records:", err);
    }
  }

  /**
   * Handle a message received from the internal message stream.
   */
  private handleStreamMessage(message: { channel: string; message: string; instanceId: string; timestamp: number }): void {
    // pass along to main handler which performs necessary checks
    const { channel, message: messageContent, instanceId, timestamp } = message;
    this.handleChannelMessage(channel, messageContent, instanceId, timestamp);
  }

  /**
   * Enable persistence for channels matching the given pattern.
   * @param pattern string or regexp pattern to match channel names
   * @param options persistence options
   */
  enableChannelPersistence(pattern: string | RegExp, options: ChannelPersistenceOptions = {}): void {
    const fullOptions: Required<ChannelPersistenceOptions> = {
      historyLimit: options.historyLimit ?? 50,
      filter: options.filter ?? (() => true),
      adapter: options.adapter ?? this.defaultAdapter,
      flushInterval: options.flushInterval ?? 500,
      maxBufferSize: options.maxBufferSize ?? 100,
    };

    // initialize custom adapter if provided and not shutting down
    if (fullOptions.adapter !== this.defaultAdapter && !this.isShuttingDown) {
      fullOptions.adapter.initialize().catch((err) => {
        serverLogger.error(`Failed to initialize adapter for pattern ${pattern}:`, err);
      });
    }

    this.channelPatterns.push({ pattern, options: fullOptions });
  }

  /**
   * Enable persistence for records matching the given pattern.
   * Use either adapter (for mesh's internal JSON blob storage) or hooks (for custom DB persistence).
   */
  enableRecordPersistence(config: RecordPersistenceConfig): void {
    serverLogger.info(`enableRecordPersistence called with config keys: ${Object.keys(config).join(", ")}`);
    serverLogger.info(`config.hooks type: ${typeof config.hooks}, config.adapter type: ${typeof config.adapter}`);
    const { pattern, adapter, hooks, flushInterval, maxBufferSize } = config;

    if (adapter && hooks) {
      throw new Error("Cannot use both adapter and hooks. Choose one.");
    }

    let resolvedAdapter: ResolvedAdapterConfig | undefined;
    if (adapter) {
      const adapterInstance = adapter.adapter ?? this.defaultAdapter;
      resolvedAdapter = { adapter: adapterInstance, restorePattern: adapter.restorePattern };

      if (adapterInstance !== this.defaultAdapter && !this.isShuttingDown) {
        adapterInstance.initialize().catch((err) => {
          serverLogger.error(`Failed to initialize adapter for record pattern ${pattern}:`, err);
        });
      }
    }

    this.recordPatterns.push({
      pattern,
      adapter: resolvedAdapter,
      hooks,
      flushInterval: flushInterval ?? 500,
      maxBufferSize: maxBufferSize ?? 100,
    });
  }

  /**
   * Check if a channel has persistence enabled and return its options.
   * @param channel channel name to check
   * @returns the persistence options if enabled, undefined otherwise
   */
  getChannelPersistenceOptions(channel: string): Required<ChannelPersistenceOptions> | undefined {
    for (const { pattern, options } of this.channelPatterns) {
      if ((typeof pattern === "string" && pattern === channel) || (pattern instanceof RegExp && pattern.test(channel))) {
        return options;
      }
    }
    return undefined;
  }

  /**
   * Check if a record has persistence enabled and return its config.
   * @param recordId record ID to check
   * @returns the persistence config if enabled, undefined otherwise
   */
  getRecordPersistenceConfig(recordId: string): RecordPatternConfig | undefined {
    for (const config of this.recordPatterns) {
      const { pattern } = config;
      if ((typeof pattern === "string" && pattern === recordId) || (pattern instanceof RegExp && pattern.test(recordId))) {
        return config;
      }
    }
    return undefined;
  }

  /**
   * Handle an incoming message for potential persistence.
   * @param channel channel the message was published to
   * @param message the message content
   * @param instanceId id of the server instance
   */
  handleChannelMessage(channel: string, message: string, instanceId: string, timestamp?: number): void {
    if (!this.initialized || this.isShuttingDown) return;

    const options = this.getChannelPersistenceOptions(channel);
    if (!options) return; // channel doesn't match any persistence pattern

    if (!options.filter(message, channel)) return; // message filtered out

    const persistedMessage: PersistedMessage = {
      id: uuidv4(),
      channel,
      message,
      instanceId,
      timestamp: timestamp || Date.now(),
    };

    if (!this.messageBuffer.has(channel)) {
      this.messageBuffer.set(channel, []);
    }
    this.messageBuffer.get(channel)!.push(persistedMessage);

    // flush if buffer reaches max size
    if (this.messageBuffer.get(channel)!.length >= options.maxBufferSize) {
      this.flushChannel(channel);
      return;
    }

    // start flush timer if not already active for this channel
    if (!this.flushTimers.has(channel)) {
      const timer = setTimeout(() => {
        this.flushChannel(channel);
      }, options.flushInterval);

      // allow process to exit even if timer is pending
      if (timer.unref) {
        timer.unref();
      }

      this.flushTimers.set(channel, timer);
    }
  }

  /**
   * Flush buffered messages for a specific channel to its adapter.
   * @param channel channel to flush
   */
  private async flushChannel(channel: string): Promise<void> {
    if (!this.messageBuffer.has(channel)) return;

    if (this.flushTimers.has(channel)) {
      clearTimeout(this.flushTimers.get(channel)!);
      this.flushTimers.delete(channel);
    }

    const messages = this.messageBuffer.get(channel)!;
    if (messages.length === 0) return;

    // clear buffer before async store to prevent duplicates on potential retry
    this.messageBuffer.set(channel, []);

    const options = this.getChannelPersistenceOptions(channel);
    if (!options) return;

    try {
      await options.adapter.storeMessages(messages);

      this.emit("flushed", { channel, count: messages.length });

      serverLogger.debug(`Flushed ${messages.length} messages for channel ${channel}`);
    } catch (err) {
      serverLogger.error(`Failed to flush messages for channel ${channel}:`, err);

      // on failure, put messages back in buffer for retry (if not shutting down)
      if (!this.isShuttingDown) {
        const currentMessages = this.messageBuffer.get(channel) || [];
        this.messageBuffer.set(channel, [...messages, ...currentMessages]);

        // schedule a retry flush
        if (!this.flushTimers.has(channel)) {
          const timer = setTimeout(() => {
            this.flushChannel(channel);
          }, 1000); // retry after 1s

          if (timer.unref) {
            timer.unref();
          }

          this.flushTimers.set(channel, timer);
        }
      }
    }
  }

  /**
   * Flush all buffered messages across all channels.
   */
  async flushAll(): Promise<void> {
    const channels = Array.from(this.messageBuffer.keys());

    for (const channel of channels) {
      await this.flushChannel(channel);
    }
  }

  /**
   * Get persisted messages for a channel.
   * @param channel channel to get messages for
   * @param since optional cursor (timestamp or message id) to retrieve messages after
   * @param limit maximum number of messages to retrieve
   */
  async getMessages(channel: string, since?: string | number, limit?: number): Promise<PersistedMessage[]> {
    if (!this.initialized) {
      throw new Error("Persistence manager not initialized");
    }

    const options = this.getChannelPersistenceOptions(channel);
    if (!options) {
      throw new Error(`Channel ${channel} does not have persistence enabled`);
    }

    // ensure pending messages are written before retrieving history
    await this.flushChannel(channel);

    return options.adapter.getMessages(channel, since, limit || options.historyLimit);
  }

  /**
   * Handles a record update for potential persistence
   * @param recordId ID of the record
   * @param value record value (will be stringified)
   * @param version record version
   */
  handleRecordUpdate(recordId: string, value: any, version: number): void {
    if (this.isShuttingDown) return;

    // buffer for later processing
    if (!this.initialized) {
      this.pendingRecordUpdates.push({ recordId, value, version });
      serverLogger.debug(`Buffered record update for ${recordId} (pending initialization)`);
      return;
    }

    const config = this.getRecordPersistenceConfig(recordId);
    if (!config) return;

    const persistedRecord: PersistedRecord = {
      recordId,
      value: JSON.stringify(value),
      version,
      timestamp: Date.now(),
    };

    this.recordBuffer.set(recordId, persistedRecord);

    serverLogger.debug(`Added record ${recordId} to buffer, buffer size: ${this.recordBuffer.size}`);

    if (this.recordBuffer.size >= config.maxBufferSize) {
      serverLogger.debug(`Buffer size ${this.recordBuffer.size} exceeds limit ${config.maxBufferSize}, flushing records`);
      this.flushRecords();
      return;
    }

    if (!this.recordFlushTimer) {
      serverLogger.debug(`Scheduling record flush in ${config.flushInterval}ms`);
      this.recordFlushTimer = setTimeout(() => {
        this.flushRecords();
      }, config.flushInterval);

      if (this.recordFlushTimer.unref) {
        this.recordFlushTimer.unref();
      }
    }
  }

  /**
   * Flush all buffered records to storage
   */
  async flushRecords(): Promise<void> {
    if (this.recordBuffer.size === 0) return;

    serverLogger.debug(`Flushing ${this.recordBuffer.size} records to storage`);

    if (this.recordFlushTimer) {
      clearTimeout(this.recordFlushTimer);
      this.recordFlushTimer = null;
    }

    const records = Array.from(this.recordBuffer.values());
    this.recordBuffer.clear();

    const recordsByAdapter = new Map<PersistenceAdapter, PersistedRecord[]>();
    const recordsByPersistFn = new Map<(records: CustomPersistedRecord[]) => Promise<void>, PersistedRecord[]>();

    for (const record of records) {
      const config = this.getRecordPersistenceConfig(record.recordId);
      if (!config) continue;

      if (config.hooks) {
        if (!recordsByPersistFn.has(config.hooks.persist)) {
          recordsByPersistFn.set(config.hooks.persist, []);
        }
        recordsByPersistFn.get(config.hooks.persist)!.push(record);
      } else if (config.adapter) {
        if (!recordsByAdapter.has(config.adapter.adapter)) {
          recordsByAdapter.set(config.adapter.adapter, []);
        }
        recordsByAdapter.get(config.adapter.adapter)!.push(record);
      }
    }

    const handleFlushError = (failedRecords: PersistedRecord[], err: unknown) => {
      serverLogger.error("Failed to flush records:", err);

      if (!this.isShuttingDown) {
        for (const record of failedRecords) {
          this.recordBuffer.set(record.recordId, record);
        }

        if (!this.recordFlushTimer) {
          this.recordFlushTimer = setTimeout(() => {
            this.flushRecords();
          }, 1000);

          if (this.recordFlushTimer.unref) {
            this.recordFlushTimer.unref();
          }
        }
      }
    };

    for (const [persistFn, persistRecords] of recordsByPersistFn.entries()) {
      try {
        const customRecords: CustomPersistedRecord[] = persistRecords.map((r) => ({
          recordId: r.recordId,
          value: JSON.parse(r.value),
          version: r.version,
        }));

        serverLogger.debug(`Storing ${persistRecords.length} records with custom persist hook`);
        await persistFn(customRecords);
        this.emit("recordsFlushed", { count: persistRecords.length });
      } catch (err) {
        handleFlushError(persistRecords, err);
      }
    }

    for (const [adapter, adapterRecords] of recordsByAdapter.entries()) {
      try {
        if (adapter.storeRecords) {
          serverLogger.debug(`Storing ${adapterRecords.length} records with adapter`);
          await adapter.storeRecords(adapterRecords);
          this.emit("recordsFlushed", { count: adapterRecords.length });
        } else {
          serverLogger.warn("Adapter does not support storing records");
        }
      } catch (err) {
        handleFlushError(adapterRecords, err);
      }
    }
  }

  /**
   * Retrieve persisted records matching a pattern
   * @param pattern pattern to match record IDs
   * @returns array of persisted records
   */
  async getPersistedRecords(pattern: string): Promise<PersistedRecord[]> {
    if (!this.initialized) {
      throw new Error("Persistence manager not initialized");
    }

    // make sure any pending records are written before trying to retrieve
    await this.flushRecords();

    try {
      const adapter = this.defaultAdapter;

      if (adapter.getRecords) {
        return await adapter.getRecords(pattern);
      }
    } catch (err) {
      serverLogger.error(`Failed to get persisted records for pattern ${pattern}:`, err);
    }

    return [];
  }

  /**
   * Shutdown the persistence manager, flushing pending messages and closing adapters.
   */
  async shutdown(): Promise<void> {
    if (this.isShuttingDown) return;

    this.isShuttingDown = true;

    this.messageStream.unsubscribeFromMessages(this.handleStreamMessage.bind(this));

    for (const timer of this.flushTimers.values()) {
      clearTimeout(timer);
    }
    this.flushTimers.clear();

    if (this.recordFlushTimer) {
      clearTimeout(this.recordFlushTimer);
      this.recordFlushTimer = null;
    }

    await this.flushAll();
    await this.flushRecords();

    const adapters = new Set<PersistenceAdapter>([this.defaultAdapter]);

    for (const { options } of this.channelPatterns) {
      adapters.add(options.adapter);
    }

    for (const config of this.recordPatterns) {
      if (config.adapter) {
        adapters.add(config.adapter.adapter);
      }
    }

    for (const adapter of adapters) {
      try {
        await adapter.close();
      } catch (err) {
        serverLogger.error("Error closing persistence adapter:", err);
      }
    }

    this.initialized = false;
  }
}
