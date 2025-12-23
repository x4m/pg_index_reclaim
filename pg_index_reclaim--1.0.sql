/* pg_index_reclaim extension */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_index_reclaim" to load this file. \quit

-- Function to reclaim space from B-tree indexes
CREATE FUNCTION reclaim_space(
    index_name regclass,
    max_pct_to_merge int DEFAULT 20
)
RETURNS TABLE(
    left_page_block bigint,
    right_page_block bigint,
    left_page_usage_pct numeric,
    right_page_usage_pct numeric,
    total_items_to_move bigint,
    estimated_space_reclaimed bigint,
    can_merge boolean
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_index_reclaim_analyze';

-- Function to actually perform the merge (future implementation)
CREATE FUNCTION reclaim_space_execute(
    index_name regclass,
    max_pct_to_merge int DEFAULT 20
)
RETURNS TABLE(
    pages_merged bigint,
    space_reclaimed bigint
)
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_index_reclaim_execute';

