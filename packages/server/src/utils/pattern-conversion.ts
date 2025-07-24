/**
 * Converts glob patterns or RegExp source strings to SQL LIKE patterns for database record queries.
 *
 * Used primarily by the persistence layer to restore records from database back to Redis on server startup.
 * Converts record ID patterns (from `enableRecordPersistence`) into SQL LIKE patterns for querying
 * persisted records from PostgreSQL/SQLite databases.
 *
 * Pattern conversion rules:
 * - `*` → `%` (glob wildcard becomes SQL zero-or-more wildcard)
 * - `?` → `_` (glob single char becomes SQL single char wildcard)
 * - `.` → `_` (regex any char becomes SQL single char wildcard)
 * - `+` → `%` (regex one-or-more becomes SQL zero-or-more wildcard)
 * - `^` and `$` → removed (SQL LIKE doesn't use anchors)
 * - Escaped chars → unescaped (e.g., `\*` → `*`, `\.` → `.`)
 *
 * @example
 * ```typescript
 * convertToSqlPattern("profile:user:*")      // → "profile:user:%"
 * convertToSqlPattern("^user:.*$")           // → "user:_%"
 * convertToSqlPattern("exact:match")         // → "exact:match"
 * ```
 *
 * @param pattern Glob pattern string or RegExp source string to convert
 * @returns SQL LIKE pattern for database queries
 */
export function convertToSqlPattern(pattern: string): string {
  return pattern
    .replace(/\^/g, "") // remove start anchor
    .replace(/\$/g, "") // remove end anchor
    .replace(/\./g, "_") // . becomes _
    .replace(/\*/g, "%") // * becomes %
    .replace(/\+/g, "%") // + becomes %
    .replace(/\?/g, "_") // ? becomes _
    .replace(/\\\\/g, "\\") // unescape backslashes
    .replace(/\\\./g, ".") // unescape dots
    .replace(/\\\*/g, "*") // unescape asterisks
    .replace(/\\\+/g, "+") // unescape plus signs
    .replace(/\\\?/g, "?");
}
