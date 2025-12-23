# Safe Testing Guide for pg_index_reclaim

## Important Notes

⚠️ **The database server crashed during testing. The extension has been updated with extensive debug logging and error handling.**

## Before Testing

1. **Check PostgreSQL logs are enabled:**
   ```sql
   SHOW log_min_messages;  -- Should be 'log' or lower
   ```

2. **Find your log file location:**
   ```sql
   SHOW log_directory;
   SHOW log_filename;
   ```

3. **Monitor logs in real-time:**
   ```bash
   tail -f /path/to/postgresql.log | grep pg_index_reclaim
   ```

## Safe Testing Steps

### Step 1: Test Analysis Only (Safe)
```sql
-- This only analyzes, doesn't modify anything
SELECT * FROM reclaim_space('idx_test_bloat_data'::regclass, 20) LIMIT 10;
```

### Step 2: Test Single Merge (Safer)
```sql
-- Get a specific merge candidate
SELECT left_page_block, right_page_block 
FROM reclaim_space('idx_test_bloat_data'::regclass, 20) 
WHERE can_merge = true 
LIMIT 1;

-- Then manually test that specific merge (requires code modification)
```

### Step 3: Test Full Execution (Use with Caution)
```sql
-- This will attempt to merge ALL candidates
-- Monitor logs closely!
SELECT * FROM reclaim_space_execute('idx_test_bloat_data'::regclass, 20);
```

## What to Look For in Logs

The extension now logs extensively. Look for:
- `pg_index_reclaim: Starting merge of pages X -> Y`
- `pg_index_reclaim: Locking left page X`
- `pg_index_reclaim: Moving N items from left page X to right page Y`
- `pg_index_reclaim: Merge of pages X -> Y completed successfully`
- Any ERROR or WARNING messages

## If Server Crashes Again

1. Check PostgreSQL logs for the last `pg_index_reclaim` log message
2. This will tell you exactly where it failed
3. Share the log output for debugging

## Recovery

If the database is corrupted:
1. Restore from backup
2. Or use `REINDEX` on the affected index
3. The extension only modifies the index, not the table data

