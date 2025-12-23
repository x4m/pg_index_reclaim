# Implementation Notes

## Current Status

### Completed
- ✅ Extension structure (Makefile, control file, SQL functions)
- ✅ Analysis function that scans index pages
- ✅ Identifies merge candidates based on usage percentage
- ✅ Validates sibling relationships
- ✅ Test script for bloat creation and measurement

### TODO
- [ ] Fix page access to use proper B-tree APIs (may need to make some functions non-static)
- [ ] Implement actual merge execution
- [ ] Add WAL logging for merge operations
- [ ] Handle posting lists
- [ ] Add proper error handling and rollback
- [ ] Test with concurrent operations
- [ ] Performance testing

## Key Design Decisions

1. **Page Access**: Currently using `ReadBufferExtended` with basic validation. May need to use internal B-tree functions or make them available to extensions.

2. **Merge Criteria**: 
   - Both pages must be under `max_pct_to_merge` usage
   - Combined items must fit in one page (90% threshold)
   - Pages must be actual siblings (checked via `btpo_next`)

3. **Locking Strategy**: 
   - Analysis phase: Read locks only
   - Execution phase: Will need write locks, following left-to-right order

4. **Space Calculation**: 
   - Uses actual item sizes from pages
   - Accounts for high key if not rightmost
   - Leaves 10% headroom for safety

## Next Steps

1. **Fix Compilation**: Ensure all B-tree access functions are available or use alternative approaches
2. **Implement Merge**: 
   - Lock pages left-to-right
   - Move items from left to right page
   - Update sibling links
   - Mark left page as deleted
   - WAL log the operation
3. **Testing**: 
   - Test with various bloat scenarios
   - Test concurrent operations
   - Test reverse scans
   - Measure performance impact

## Known Issues

1. May need to access internal B-tree functions - might require making some functions non-static or creating extension API
2. Need to handle incomplete splits
3. Need to coordinate with VACUUM
4. Need to handle posting lists properly

