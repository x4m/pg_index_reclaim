# Debugging Guide for pg_index_reclaim

## Current Status

The extension is crashing during merge execution. The analysis function works correctly, but `reclaim_space_execute` causes the server to crash.

## Recent Changes

1. ✅ Fixed B-tree traversal to go from root → leftmost leaf → follow siblings
2. ✅ Added reverse insertion order for items (to avoid offset shifting)
3. ✅ Added extensive debug logging
4. ✅ Limited to 10 merges per execution
5. ✅ Added error handling with PG_TRY/PG_CATCH

## Known Issues

The server crashes when executing merges. Possible causes:

1. **WAL Logging Issue**: Using `XLOG_BTREE_DELETE` for a merge operation might not be correct
2. **High Key Handling**: The logic for updating high keys might be incorrect
3. **Page State**: We might not be updating page state correctly after merging
4. **Sibling Links**: The sibling link updates might be incorrect

## Next Steps to Debug

1. **Check PostgreSQL logs** - Look for the last `pg_index_reclaim` log message before crash
2. **Simplify WAL logging** - Maybe skip WAL for now to isolate the issue
3. **Test with single merge** - Process only one merge at a time
4. **Add assertions** - Add more validation checks before critical operations

## Log File Location

Logs should be in: `/home/kwolak/pgsql/17/data/log/`

To find the latest log:
```bash
find /home/kwolak/pgsql/17/data/log -name "*.log" -type f | xargs ls -t | head -1
```

To monitor:
```bash
/home/kwolak/postgres/contrib/pg_index_reclaim/monitor_logs.sh
```

## Testing Commands

```sql
-- Safe: Just analyze
SELECT * FROM reclaim_space('idx_test_bloat_data'::regclass, 20) LIMIT 5;

-- Dangerous: Execute (causes crash currently)
SELECT * FROM reclaim_space_execute('idx_test_bloat_data'::regclass, 20);
```

