# pg_index_reclaim

## Overview

`pg_index_reclaim` is a PostgreSQL extension that reclaims space from B-tree indexes by merging underutilized pages without requiring a full index rebuild.

## Features

- **Non-blocking**: Works without requiring exclusive locks on the index
- **Incremental**: Can be run repeatedly to gradually reduce bloat
- **Safe**: Uses the same locking mechanisms as VACUUM
- **Efficient**: Only processes pages that meet merge criteria

## Installation

```sql
CREATE EXTENSION pg_index_reclaim;
```

## Usage

### Analyze Merge Candidates

Analyze an index to find pages that can be merged:

```sql
SELECT * FROM reclaim_space('index_name', max_pct_to_merge);
```

Parameters:
- `index_name`: Name of the B-tree index to analyze
- `max_pct_to_merge`: Maximum page usage percentage to consider for merging (default: 20)

Returns:
- `left_page_block`: Block number of the left page
- `right_page_block`: Block number of the right page  
- `left_page_usage_pct`: Usage percentage of left page
- `right_page_usage_pct`: Usage percentage of right page
- `total_items_to_move`: Total number of items to move
- `estimated_space_reclaimed`: Estimated space that would be reclaimed
- `can_merge`: Whether the merge is feasible

### Execute Merge (Future)

```sql
SELECT * FROM reclaim_space_execute('index_name', max_pct_to_merge);
```

## How It Works

1. **Analysis Phase**: Scans the index to identify adjacent pages that are both underutilized
2. **Merge Phase**: Moves items from the left page to the right page, then marks the left page as deleted
3. **Cleanup**: VACUUM will eventually reclaim the empty pages

## Design Principles

- Items only move right (never left) - maintains B-tree invariants
- Empty pages are left for VACUUM to delete - follows existing patterns
- Reverse scans are handled automatically - existing recovery logic works
- Locking follows left-to-right order - prevents deadlocks

## Limitations

- Currently only analyzes leaf pages
- Requires B-tree indexes (version 4+)
- Does not handle posting lists yet
- Merge execution not yet implemented

## Testing

See `test_bloat.sql` for a complete test script that:
1. Creates a table with 1M rows
2. Deletes 80% to create bloat
3. Analyzes bloat
4. Runs reclaim analysis
5. (Future) Executes merge and measures results

## Status

**Work in Progress**

- [x] Extension structure
- [x] Analysis function
- [ ] Merge execution function
- [ ] WAL logging
- [ ] Error handling
- [ ] Testing

