-- Test script for pg_index_reclaim extension
-- Creates a table with 1M rows, deletes 80%, measures bloat, runs reclaim, measures again

\echo 'Creating test table with 1 million rows...'

-- Create test table
DROP TABLE IF EXISTS test_bloat_table CASCADE;
CREATE TABLE test_bloat_table (
    id SERIAL PRIMARY KEY,
    data NUMERIC(20,10)
);

-- Insert 1 million rows with random numeric data
INSERT INTO test_bloat_table (data)
SELECT (random() * 1000000)::numeric(20,10)
FROM generate_series(1, 1000000);

-- Create index
CREATE INDEX idx_test_bloat_data ON test_bloat_table(data);

-- Analyze to update statistics
ANALYZE test_bloat_table;

\echo 'Initial index size:'
SELECT 
    pg_size_pretty(pg_relation_size('idx_test_bloat_data')) AS index_size,
    pg_relation_size('idx_test_bloat_data') AS index_size_bytes;

\echo 'Deleting 80% of rows to create bloat...'

-- Delete 80% of rows (randomly)
DELETE FROM test_bloat_table
WHERE random() < 0.8;

-- Vacuum to mark space as free (but don't reclaim yet)
VACUUM ANALYZE test_bloat_table;

\echo 'After deletion (before reclaim):'
SELECT 
    pg_size_pretty(pg_relation_size('idx_test_bloat_data')) AS index_size,
    pg_relation_size('idx_test_bloat_data') AS index_size_bytes;

-- Query bloat using simple method
\echo 'Bloat analysis (before reclaim):'
SELECT
    schemaname,
    relname AS tablename,
    indexrelname AS indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    pg_relation_size(indexrelid) AS index_size_bytes,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE indexrelname = 'idx_test_bloat_data';

-- Get index statistics
\echo 'Index statistics:'
SELECT 
    relname,
    relpages,
    reltuples,
    pg_size_pretty(pg_relation_size(oid)) AS size
FROM pg_class
WHERE relname = 'idx_test_bloat_data';

-- Run our reclaim analysis
\echo 'Analyzing merge candidates (max 20% usage threshold):'
SELECT * FROM reclaim_space('idx_test_bloat_data', 20)
ORDER BY left_page_block
LIMIT 20;

-- Count total merge candidates
\echo 'Total merge candidates found:'
SELECT COUNT(*) AS total_candidates,
       COUNT(*) FILTER (WHERE can_merge) AS mergeable_candidates
FROM reclaim_space('idx_test_bloat_data', 20);

\echo 'Test completed. Run reclaim_space_execute() when ready to perform merges.';

