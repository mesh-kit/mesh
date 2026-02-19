export interface PersistenceAdapter {
  initialize(): Promise<void>;
  storeMessages(messages: PersistedMessage[]): Promise<void>;
  getMessages(channel: string, since?: string | number, limit?: number): Promise<PersistedMessage[]>;
  storeRecords?(records: PersistedRecord[]): Promise<void>;
  getRecords?(pattern: string): Promise<PersistedRecord[]>;
  close(): Promise<void>;
}

export interface PersistedMessage {
  id: string;
  channel: string;
  message: string;
  instanceId: string;
  timestamp: number;
  metadata?: Record<string, any>;
}

export interface ChannelPersistenceOptions {
  /**
   * Maximum number of messages to retain per channel
   * @default 50
   */
  historyLimit?: number;

  /**
   * Function to filter messages for persistence
   * Return false to skip persistence for a specific message
   */
  filter?: (message: string, channel: string) => boolean;

  /**
   * Optional adapter override for this pattern
   */
  adapter?: PersistenceAdapter;

  /**
   * How often (in ms) to flush buffered messages to the database
   * @default 500
   */
  flushInterval?: number;

  /**
   * Maximum number of messages to hold in memory per channel
   * If this limit is reached, the buffer is flushed immediately
   * @default 100
   */
  maxBufferSize?: number;
}

export interface RecordPersistenceAdapterConfig {
  adapter?: PersistenceAdapter;
  restorePattern: string;
}

export interface RecordPersistenceHooksConfig {
  persist: (records: CustomPersistedRecord[]) => Promise<void>;
  restore: () => Promise<CustomPersistedRecord[]>;
}

export interface RecordPersistenceConfig {
  pattern: string | RegExp;
  adapter?: RecordPersistenceAdapterConfig;
  hooks?: RecordPersistenceHooksConfig;
  flushInterval?: number;
  maxBufferSize?: number;
}

export interface PersistedRecord {
  recordId: string;
  version: number;
  value: string;
  timestamp: number;
}

export interface CustomPersistedRecord {
  recordId: string;
  value: unknown;
  version: number;
}

export interface PersistenceAdapterOptions {
  /**
   * Database file path for file-based adapters
   * @default ":memory:"
   */
  filename?: string;
}

export interface PostgreSQLAdapterOptions extends PersistenceAdapterOptions {
  connectionString?: string;
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  ssl?: boolean;
  max?: number;
}
