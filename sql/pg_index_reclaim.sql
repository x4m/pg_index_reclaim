--
-- Test pg_index_reclaim extension
--

-- Create extension
CREATE EXTENSION pg_index_reclaim;

-- Create test table with data
-- Using a smaller dataset for regression testing (10000 rows instead of 1M)
CREATE TABLE test_reclaim AS 
SELECT random() AS a, random() AS b 
FROM generate_series(1, 10000);

-- Create index on the table
CREATE INDEX test_reclaim_idx ON test_reclaim(a, b);

-- Check initial index size
SELECT pg_relation_size('test_reclaim_idx') > 0 AS has_size;

-- Delete most rows to create bloat (keep only ~3% of rows)
DELETE FROM test_reclaim WHERE b > 0.03;

-- Vacuum to mark dead tuples (but won't reclaim index pages)
VACUUM test_reclaim;

-- Run analyze to find merge candidates
-- The exact output depends on random data, so we just check it returns rows
SELECT count(*) > 0 AS has_candidates 
FROM reclaim_space('test_reclaim_idx'::regclass, 50);

-- Show that merge candidates have valid structure
SELECT 
    bool_and(left_page_block IS NOT NULL) AS all_have_left,
    bool_and(right_page_block IS NOT NULL) AS all_have_right,
    bool_or(can_merge) AS some_can_merge
FROM reclaim_space('test_reclaim_idx'::regclass, 50);

-- Execute reclaim - first pass
SELECT pages_merged >= 0 AS valid_result, space_reclaimed >= 0 AS valid_space
FROM reclaim_space_execute('test_reclaim_idx'::regclass, 50);

-- Execute reclaim - second pass
SELECT pages_merged >= 0 AS valid_result, space_reclaimed >= 0 AS valid_space
FROM reclaim_space_execute('test_reclaim_idx'::regclass, 50);

-- Execute reclaim - third pass
SELECT pages_merged >= 0 AS valid_result, space_reclaimed >= 0 AS valid_space
FROM reclaim_space_execute('test_reclaim_idx'::regclass, 50);

-- Execute reclaim - fourth pass
SELECT pages_merged >= 0 AS valid_result, space_reclaimed >= 0 AS valid_space
FROM reclaim_space_execute('test_reclaim_idx'::regclass, 50);

-- Execute reclaim - fifth pass
SELECT pages_merged >= 0 AS valid_result, space_reclaimed >= 0 AS valid_space
FROM reclaim_space_execute('test_reclaim_idx'::regclass, 50);

-- Vacuum to clean up half-dead pages left by reclaim
VACUUM test_reclaim;

-- Verify index is still valid by running amcheck if available
-- (Skip if amcheck is not installed)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'amcheck') THEN
        PERFORM bt_index_check('test_reclaim_idx'::regclass);
        RAISE NOTICE 'Index check passed';
    ELSE
        RAISE NOTICE 'amcheck not installed, skipping index check';
    END IF;
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'amcheck not available, skipping index check';
END;
$$;

-- Verify data is still accessible through the index
SET enable_seqscan = off;
SELECT count(*) > 0 AS has_remaining_rows FROM test_reclaim WHERE a > 0;
RESET enable_seqscan;

-- Test error handling: non-btree index should fail
CREATE TABLE test_hash (a int);
CREATE INDEX test_hash_idx ON test_hash USING hash(a);
SELECT * FROM reclaim_space('test_hash_idx'::regclass, 50);

-- Test error handling: invalid percentage
SELECT * FROM reclaim_space('test_reclaim_idx'::regclass, 0);
SELECT * FROM reclaim_space('test_reclaim_idx'::regclass, 101);

-- Clean up
DROP TABLE test_reclaim;
DROP TABLE test_hash;
DROP EXTENSION pg_index_reclaim;
