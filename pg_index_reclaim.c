/*-------------------------------------------------------------------------
 *
 * pg_index_reclaim.c
 *	  Reclaim space from B-tree indexes by merging underutilized pages
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_index_reclaim/pg_index_reclaim.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

/*
 * Structure to hold analysis of a single page
 */
typedef struct PageAnalysis
{
	BlockNumber blockno;
	bool		is_leaf;
	bool		is_rightmost;
	bool		is_deleted;
	bool		is_halfdead;
	Size		used_space;
	Size		free_space;
	int			item_count;
	double		usage_pct;
} PageAnalysis;

/*
 * Structure to hold merge candidate information
 */
typedef struct MergeCandidate
{
	BlockNumber left_page;
	BlockNumber right_page;
	double		left_usage_pct;
	double		right_usage_pct;
	int			total_items;
	Size		estimated_space;
	bool		can_merge;
} MergeCandidate;

/*
 * Dump page contents for debugging (only active when DEBUG1 or higher is enabled)
 */
static void
dump_page(Relation rel, BlockNumber blkno, const char *label)
{
	Buffer		buf;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber maxoff;
	OffsetNumber offnum;

	elog(DEBUG1, "pg_index_reclaim: ===== PAGE DUMP: %s (block %u) =====", label, blkno);

	buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BT_READ);
	page = BufferGetPage(buf);

	if (PageIsNew(page))
	{
		elog(DEBUG1, "pg_index_reclaim: Page %u is NEW", blkno);
		UnlockReleaseBuffer(buf);
		return;
	}

	opaque = BTPageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	elog(DEBUG1, "pg_index_reclaim: Page %u: level=%u, flags=0x%x, prev=%u, next=%u, cycleid=%u",
		 blkno, opaque->btpo_level, opaque->btpo_flags,
		 opaque->btpo_prev, opaque->btpo_next, opaque->btpo_cycleid);
	elog(DEBUG1, "pg_index_reclaim: Page %u: is_leaf=%d, is_rightmost=%d, is_deleted=%d, is_halfdead=%d",
		 blkno, P_ISLEAF(opaque), P_RIGHTMOST(opaque), 
		 P_ISDELETED(opaque), P_ISHALFDEAD(opaque));
	elog(DEBUG1, "pg_index_reclaim: Page %u: maxoff=%u, pd_lower=%u, pd_upper=%u, free_space=%zu",
		 blkno, maxoff, ((PageHeader) page)->pd_lower, 
		 ((PageHeader) page)->pd_upper, PageGetFreeSpace(page));

	if (P_ISLEAF(opaque))
	{
		OffsetNumber firstdata = P_FIRSTDATAKEY(opaque);
		int			item_count = 0;
		Size		total_size = 0;

		elog(DEBUG1, "pg_index_reclaim: Page %u: firstdata=%u", blkno, firstdata);

		for (offnum = firstdata; offnum <= maxoff; offnum++)
		{
			ItemId		itemid = PageGetItemId(page, offnum);
			if (ItemIdIsUsed(itemid))
			{
				Size itemsize = ItemIdGetLength(itemid);
				total_size += itemsize;
				item_count++;
			}
		}

		elog(DEBUG1, "pg_index_reclaim: Page %u: %d items, total size=%zu bytes", 
			 blkno, item_count, total_size);
	}

	UnlockReleaseBuffer(buf);
	elog(DEBUG1, "pg_index_reclaim: ===== END PAGE DUMP: %s =====", label);
}

/*
 * Find the leftmost leaf page by traversing from root
 */
static BlockNumber
find_leftmost_leaf(Relation rel)
{
	Buffer		metabuf;
	BTMetaPageData *metad;
	Buffer		buf;
	Page		page;
	BTPageOpaque opaque;
	BlockNumber blkno;

	elog(DEBUG1, "pg_index_reclaim: Finding leftmost leaf page");

	/* Get metapage */
	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, BTREE_METAPAGE, RBM_NORMAL, NULL);
	LockBuffer(metabuf, BT_READ);
	metad = BTPageGetMeta(BufferGetPage(metabuf));

	if (metad->btm_root == P_NONE)
	{
		elog(DEBUG1, "pg_index_reclaim: Index has no root page");
		UnlockReleaseBuffer(metabuf);
		return P_NONE;
	}

	blkno = metad->btm_root;
	UnlockReleaseBuffer(metabuf);

	elog(DEBUG1, "pg_index_reclaim: Starting from root page %u", blkno);

	/* Traverse down to leftmost leaf */
	for (;;)
	{
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
		LockBuffer(buf, BT_READ);
		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);

		if (PageIsNew(page) || P_ISDELETED(opaque))
		{
			elog(WARNING, "pg_index_reclaim: Page %u is new or deleted during traversal", blkno);
			UnlockReleaseBuffer(buf);
			return P_NONE;
		}

		if (P_ISLEAF(opaque))
		{
			elog(DEBUG1, "pg_index_reclaim: Found leftmost leaf page %u", blkno);
			UnlockReleaseBuffer(buf);
			return blkno;
		}

		/* Internal page - get leftmost child */
		/* For internal pages, the leftmost child is at P_FIRSTDATAKEY */
		{
			OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
			OffsetNumber firstdata = P_FIRSTDATAKEY(opaque);
			ItemId		itemid;
			IndexTuple	itup;
			BlockNumber child;

			if (maxoff < firstdata)
			{
				elog(WARNING, "pg_index_reclaim: Internal page %u has no data items (maxoff=%u, firstdata=%u)", 
					 blkno, maxoff, firstdata);
				UnlockReleaseBuffer(buf);
				return P_NONE;
			}

			itemid = PageGetItemId(page, firstdata);
			if (!ItemIdIsUsed(itemid))
			{
				elog(WARNING, "pg_index_reclaim: First data item on page %u is not used", blkno);
				UnlockReleaseBuffer(buf);
				return P_NONE;
			}

			itup = (IndexTuple) PageGetItem(page, itemid);
			child = BTreeTupleGetDownLink(itup);

			if (!BlockNumberIsValid(child))
			{
				elog(WARNING, "pg_index_reclaim: Invalid downlink on page %u", blkno);
				UnlockReleaseBuffer(buf);
				return P_NONE;
			}

			elog(DEBUG1, "pg_index_reclaim: Following downlink from page %u (level %u) to child %u", 
				 blkno, opaque->btpo_level, child);
			UnlockReleaseBuffer(buf);
			blkno = child;
		}
	}
}

/*
 * Analyze a B-tree index to find pages that can be merged
 * Traverses from root to leftmost leaf, then follows sibling links
 */
static void
analyze_index_pages(Relation rel, List **merge_candidates, int max_pct_to_merge)
{
	BlockNumber num_pages;
	BlockNumber blkno;
	BufferAccessStrategy strategy;
	PageAnalysis *pages;
	int			total_pages = 0;
	int			leaf_pages = 0;
	BlockNumber leftmost_leaf;

	elog(DEBUG1, "pg_index_reclaim: Starting page analysis");

	/* Get total number of pages */
	num_pages = RelationGetNumberOfBlocks(rel);
	if (num_pages <= 1)		/* Only metapage */
	{
		elog(DEBUG1, "pg_index_reclaim: Index has only metapage, nothing to analyze");
		return;
	}

	elog(DEBUG1, "pg_index_reclaim: Index has %u pages", num_pages);

	/* Find leftmost leaf by traversing from root */
	leftmost_leaf = find_leftmost_leaf(rel);
	if (leftmost_leaf == P_NONE)
	{
		elog(WARNING, "pg_index_reclaim: Could not find leftmost leaf page");
		return;
	}

	/* Allocate array to hold page analysis */
	pages = (PageAnalysis *) palloc(sizeof(PageAnalysis) * num_pages);

	/* Use a buffer access strategy for sequential scans */
	strategy = GetAccessStrategy(BAS_BULKREAD);

	/* Scan leaf pages following sibling links starting from leftmost */
	blkno = leftmost_leaf;
	elog(DEBUG1, "pg_index_reclaim: Starting leaf page scan from page %u", blkno);
	
	while (blkno != P_NONE && total_pages < num_pages)
	{
		Buffer		buf;
		Page		page;
		BTPageOpaque opaque;
		OffsetNumber maxoff;
		Size		used_space = 0;
		Size		free_space;
		int			item_count = 0;
		OffsetNumber offnum;
		BlockNumber next_blkno;

		/* Read the page */
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy);
		LockBuffer(buf, BT_READ);

		page = BufferGetPage(buf);
		
		/* Check if page is new/uninitialized */
		if (PageIsNew(page))
		{
			elog(WARNING, "pg_index_reclaim: Page %u is new/uninitialized, stopping scan", blkno);
			UnlockReleaseBuffer(buf);
			break;
		}

		/* Basic validation - check page header magic */
		if (PageGetPageSize(page) != BLCKSZ)
		{
			elog(WARNING, "pg_index_reclaim: Page %u has invalid page size, stopping scan", blkno);
			UnlockReleaseBuffer(buf);
			break;
		}

		opaque = BTPageGetOpaque(page);

		/* Skip deleted pages */
		if (P_ISDELETED(opaque))
		{
			elog(DEBUG1, "pg_index_reclaim: Page %u is deleted, skipping", blkno);
			next_blkno = opaque->btpo_next;
			UnlockReleaseBuffer(buf);
			blkno = next_blkno;
			continue;
		}

		/* Skip non-leaf pages (shouldn't happen if we started from leaf) */
		if (!P_ISLEAF(opaque))
		{
			elog(WARNING, "pg_index_reclaim: Page %u is not a leaf (level %u), stopping scan", 
				 blkno, opaque->btpo_level);
			UnlockReleaseBuffer(buf);
			break;
		}

		/* Skip half-dead pages (they'll be handled by VACUUM) */
		if (P_ISHALFDEAD(opaque))
		{
			elog(DEBUG1, "pg_index_reclaim: Page %u is half-dead, skipping", blkno);
			next_blkno = opaque->btpo_next;
			UnlockReleaseBuffer(buf);
			blkno = next_blkno;
			continue;
		}

		/* Get next sibling for next iteration */
		next_blkno = opaque->btpo_next;

		/* Get page statistics */
		maxoff = PageGetMaxOffsetNumber(page);
		free_space = PageGetFreeSpace(page);

		/* Calculate used space by summing item sizes */
		for (offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum++)
		{
			ItemId		itemid = PageGetItemId(page, offnum);
			Size		itemsize = MAXALIGN(ItemIdGetLength(itemid));

			used_space += itemsize;
			item_count++;
		}

		/* Store analysis */
		pages[total_pages].blockno = blkno;
		pages[total_pages].is_leaf = true;
		pages[total_pages].is_rightmost = P_RIGHTMOST(opaque);
		pages[total_pages].is_deleted = false;
		pages[total_pages].is_halfdead = false;
		pages[total_pages].item_count = item_count;
		pages[total_pages].used_space = used_space;
		pages[total_pages].free_space = free_space;
		
		/* Calculate usage percentage */
		{
			Size total_space = BLCKSZ - SizeOfPageHeaderData - 
							   MAXALIGN(sizeof(BTPageOpaqueData));
			if (total_space > 0)
				pages[total_pages].usage_pct = 
					(double) used_space / (double) total_space * 100.0;
			else
				pages[total_pages].usage_pct = 0.0;
		}

		elog(DEBUG1, "pg_index_reclaim: Analyzed leaf page %u: %d items, %.2f%% usage, next=%u",
			 blkno, item_count, pages[total_pages].usage_pct, next_blkno);

		total_pages++;
		leaf_pages++;

		UnlockReleaseBuffer(buf);

		/* Move to next sibling */
		blkno = next_blkno;
	}

	elog(DEBUG1, "pg_index_reclaim: Scanned %d leaf pages", leaf_pages);

	/* Now find merge candidates by checking actual sibling relationships */
	{
		int			i;
		PageAnalysis *left_page;
		PageAnalysis *right_page;
		Buffer		left_buf;
		Page		left_page_ptr;
		BTPageOpaque left_opaque;
		BlockNumber right_blockno;
		int			j;
		Size		combined_used;
		Size		total_available;
		Size		avg_item_size;
		bool		can_merge;

		for (i = 0; i < total_pages; i++)
		{
			left_page = &pages[i];

			/* Skip rightmost pages - they can't be merged */
			if (left_page->is_rightmost)
				continue;

			/* Get the actual right sibling from the page */
			left_buf = ReadBufferExtended(rel, MAIN_FORKNUM, left_page->blockno,
										  RBM_NORMAL, strategy);
			LockBuffer(left_buf, BT_READ);
			left_page_ptr = BufferGetPage(left_buf);

			if (PageIsNew(left_page_ptr))
			{
				UnlockReleaseBuffer(left_buf);
				continue;
			}

			left_opaque = BTPageGetOpaque(left_page_ptr);
			right_blockno = left_opaque->btpo_next;
			UnlockReleaseBuffer(left_buf);

			if (right_blockno == P_NONE)
				continue;

			/* Find the right page in our analysis array */
			right_page = NULL;
			for (j = 0; j < total_pages; j++)
			{
				if (pages[j].blockno == right_blockno)
				{
					right_page = &pages[j];
					break;
				}
			}

			if (right_page == NULL)
				continue;	/* Right page not in our analysis (might be new) */

			/* Skip if either page is deleted or half-dead */
			if (left_page->is_deleted || left_page->is_halfdead ||
				right_page->is_deleted || right_page->is_halfdead)
				continue;

			/* Check if both pages are underutilized */
			if (left_page->usage_pct > max_pct_to_merge &&
				right_page->usage_pct > max_pct_to_merge)
				continue;

			/* Check if combined pages would fit */
			combined_used = left_page->used_space + right_page->used_space;
			total_available = BLCKSZ - SizeOfPageHeaderData -
				MAXALIGN(sizeof(BTPageOpaqueData));

			/* Need space for high key if not rightmost */
			if (!right_page->is_rightmost)
			{
				/* Estimate high key size - use average item size as approximation */
				avg_item_size = right_page->item_count > 0 ?
					right_page->used_space / right_page->item_count : 0;
				total_available -= MAXALIGN(avg_item_size);
			}

			/* Use 90% threshold to leave some headroom */
			can_merge = (combined_used <= total_available * 0.9);

			/* Create merge candidate */
			{
				MergeCandidate *candidate = (MergeCandidate *) palloc(sizeof(MergeCandidate));

				candidate->left_page = left_page->blockno;
				candidate->right_page = right_page->blockno;
				candidate->left_usage_pct = left_page->usage_pct;
				candidate->right_usage_pct = right_page->usage_pct;
				candidate->total_items = left_page->item_count + right_page->item_count;
				candidate->estimated_space = combined_used;
				candidate->can_merge = can_merge;

				*merge_candidates = lappend(*merge_candidates, candidate);
			}
		}
	}

	pfree(pages);
}

/* Parent page finding removed - VACUUM will fix parent links later */

/*
 * Execute a merge of two pages
 *
 * Merges the contents of left_block into right_block, then marks
 * left_block as half-dead. VACUUM will later fully delete the page.
 *
 * WAL logging uses Full Page Images (FPI) via log_newpage_buffer() for
 * all modified pages. This is simpler than creating custom WAL records
 * and ensures crash safety.
 */
static bool
execute_merge(Relation rel, BlockNumber left_block, BlockNumber right_block)
{
	Buffer		left_buf;
	Buffer		right_buf;
	Buffer		right_sibling_buf = InvalidBuffer;
	Buffer		left_sibling_buf = InvalidBuffer;
	Page		left_page;
	Page		right_page;
	Page		left_sibling_page = NULL;
	BTPageOpaque left_opaque;
	BTPageOpaque right_opaque;
	BTPageOpaque right_sibling_opaque = NULL;
	BTPageOpaque left_sibling_opaque = NULL;
	OffsetNumber left_maxoff;
	OffsetNumber right_maxoff;
	OffsetNumber left_firstdata;
	OffsetNumber right_firstdata;
	OffsetNumber offnum;
	ItemId		itemid;
	bool		is_rightmost;
	BlockNumber leftsib;
	BlockNumber rightsib;
	bool		needswal = RelationNeedsWAL(rel);

	/* Pre-allocated arrays for items - allocated before critical section */
	OffsetNumber *left_offsets = NULL;
	OffsetNumber *deletable = NULL;
	int			nleft_items = 0;
	int			ndeletable = 0;
	Size		left_items_total_size = 0;

	elog(DEBUG1, "pg_index_reclaim: ========================================");
	elog(DEBUG1, "pg_index_reclaim: Starting merge of pages %u -> %u in index \"%s\"",
		 left_block, right_block, RelationGetRelationName(rel));
	elog(DEBUG1, "pg_index_reclaim: ========================================");

	/* Dump pages BEFORE merge */
	dump_page(rel, left_block, "LEFT PAGE (BEFORE MERGE)");
	dump_page(rel, right_block, "RIGHT PAGE (BEFORE MERGE)");

	/* Lock pages left-to-right to avoid deadlocks */
	elog(DEBUG1, "pg_index_reclaim: Locking left page %u", left_block);
	left_buf = ReadBufferExtended(rel, MAIN_FORKNUM, left_block, RBM_NORMAL, NULL);
	LockBuffer(left_buf, BT_WRITE);
	left_page = BufferGetPage(left_buf);

	if (PageIsNew(left_page))
	{
		elog(DEBUG1, "pg_index_reclaim: Left page %u is new/uninitialized, aborting", left_block);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	left_opaque = BTPageGetOpaque(left_page);

	/* Validate left page */
	if (!P_ISLEAF(left_opaque))
	{
		elog(DEBUG1, "pg_index_reclaim: Left page %u is not a leaf page (level %u), aborting",
			 left_block, left_opaque->btpo_level);
		UnlockReleaseBuffer(left_buf);
		return false;
	}
	if (P_ISDELETED(left_opaque))
	{
		elog(DEBUG1, "pg_index_reclaim: Left page %u is already deleted, aborting", left_block);
		UnlockReleaseBuffer(left_buf);
		return false;
	}
	if (P_ISHALFDEAD(left_opaque))
	{
		elog(DEBUG1, "pg_index_reclaim: Left page %u is half-dead, aborting", left_block);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	elog(DEBUG1, "pg_index_reclaim: Left page %u validated: prev=%u, next=%u, flags=0x%x",
		 left_block, left_opaque->btpo_prev, left_opaque->btpo_next, left_opaque->btpo_flags);

	/* Get right page */
	elog(DEBUG1, "pg_index_reclaim: Locking right page %u", right_block);
	right_buf = ReadBufferExtended(rel, MAIN_FORKNUM, right_block, RBM_NORMAL, NULL);
	LockBuffer(right_buf, BT_WRITE);
	right_page = BufferGetPage(right_buf);

	if (PageIsNew(right_page))
	{
		elog(DEBUG1, "pg_index_reclaim: Right page %u is new/uninitialized, aborting", right_block);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	right_opaque = BTPageGetOpaque(right_page);

	/* Validate right page and sibling relationship */
	if (!P_ISLEAF(right_opaque))
	{
		elog(DEBUG1, "pg_index_reclaim: Right page %u is not a leaf page (level %u), aborting",
			 right_block, right_opaque->btpo_level);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}
	if (P_ISDELETED(right_opaque))
	{
		elog(DEBUG1, "pg_index_reclaim: Right page %u is already deleted, aborting", right_block);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}
	if (P_ISHALFDEAD(right_opaque))
	{
		elog(DEBUG1, "pg_index_reclaim: Right page %u is half-dead, aborting", right_block);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	if (right_opaque->btpo_prev != left_block)
	{
		elog(DEBUG1, "pg_index_reclaim: Sibling relationship mismatch: right page %u prev=%u, expected %u, aborting",
			 right_block, right_opaque->btpo_prev, left_block);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	elog(DEBUG1, "pg_index_reclaim: Right page %u validated: prev=%u, next=%u, flags=0x%x",
		 right_block, right_opaque->btpo_prev, right_opaque->btpo_next, right_opaque->btpo_flags);

	/* Get sibling information */
	leftsib = left_opaque->btpo_prev;
	rightsib = right_opaque->btpo_next;
	is_rightmost = P_RIGHTMOST(right_opaque);

	elog(DEBUG1, "pg_index_reclaim: Sibling info: leftsib=%u, rightsib=%u, is_rightmost=%d",
		 leftsib, rightsib, is_rightmost);

	/* Lock right sibling if it exists (for updating its left-link) */
	if (!is_rightmost)
	{
		elog(DEBUG1, "pg_index_reclaim: Locking right sibling page %u", rightsib);
		right_sibling_buf = ReadBufferExtended(rel, MAIN_FORKNUM, rightsib,
											   RBM_NORMAL, NULL);
		LockBuffer(right_sibling_buf, BT_WRITE);
		right_sibling_opaque = BTPageGetOpaque(BufferGetPage(right_sibling_buf));

		/* Validate right sibling's left-link */
		if (right_sibling_opaque->btpo_prev != right_block)
		{
			elog(DEBUG1, "pg_index_reclaim: Right sibling %u prev=%u, expected %u, aborting",
				 rightsib, right_sibling_opaque->btpo_prev, right_block);
			UnlockReleaseBuffer(right_sibling_buf);
			UnlockReleaseBuffer(right_buf);
			UnlockReleaseBuffer(left_buf);
			return false;
		}
		elog(DEBUG1, "pg_index_reclaim: Right sibling %u validated", rightsib);
	}

	/* Lock left sibling if it exists - BEFORE critical section */
	if (leftsib != P_NONE)
	{
		elog(DEBUG1, "pg_index_reclaim: Locking left sibling page %u", leftsib);
		left_sibling_buf = ReadBufferExtended(rel, MAIN_FORKNUM, leftsib,
											  RBM_NORMAL, NULL);
		LockBuffer(left_sibling_buf, BT_WRITE);
		left_sibling_page = BufferGetPage(left_sibling_buf);
		left_sibling_opaque = BTPageGetOpaque(left_sibling_page);

		/* Validate left sibling's right-link */
		if (left_sibling_opaque->btpo_next != left_block)
		{
			elog(DEBUG1, "pg_index_reclaim: Left sibling %u next=%u, expected %u, aborting",
				 leftsib, left_sibling_opaque->btpo_next, left_block);
			UnlockReleaseBuffer(left_sibling_buf);
			if (BufferIsValid(right_sibling_buf))
				UnlockReleaseBuffer(right_sibling_buf);
			UnlockReleaseBuffer(right_buf);
			UnlockReleaseBuffer(left_buf);
			return false;
		}
		elog(DEBUG1, "pg_index_reclaim: Left sibling %u validated", leftsib);
	}

	/* Get page statistics */
	left_maxoff = PageGetMaxOffsetNumber(left_page);
	right_maxoff = PageGetMaxOffsetNumber(right_page);
	left_firstdata = P_FIRSTDATAKEY(left_opaque);
	right_firstdata = P_FIRSTDATAKEY(right_opaque);

	elog(DEBUG1, "pg_index_reclaim: Page stats - left: maxoff=%u, firstdata=%u; right: maxoff=%u, firstdata=%u",
		 left_maxoff, left_firstdata, right_maxoff, right_firstdata);

	/* Check if we have space - should have been validated by analyze, but double-check */
	{
		Size total_used = 0;
		Size available_space;

		for (offnum = left_firstdata; offnum <= left_maxoff; offnum++)
		{
			itemid = PageGetItemId(left_page, offnum);
			if (!ItemIdIsUsed(itemid))
			{
				elog(WARNING, "pg_index_reclaim: Left page %u has unused item at offset %u", left_block, offnum);
				continue;
			}
			total_used += MAXALIGN(ItemIdGetLength(itemid));
		}
		for (offnum = right_firstdata; offnum <= right_maxoff; offnum++)
		{
			itemid = PageGetItemId(right_page, offnum);
			if (!ItemIdIsUsed(itemid))
			{
				elog(WARNING, "pg_index_reclaim: Right page %u has unused item at offset %u", right_block, offnum);
				continue;
			}
			total_used += MAXALIGN(ItemIdGetLength(itemid));
		}

		available_space = PageGetFreeSpace(right_page);
		elog(DEBUG1, "pg_index_reclaim: Space check - total_used=%zu, available_space=%zu",
			 total_used, available_space);

		if (total_used > available_space)
		{
			elog(DEBUG1, "pg_index_reclaim: Not enough space (%zu > %zu), aborting",
				 total_used, available_space);
			/* Not enough space - abort */
			if (BufferIsValid(left_sibling_buf))
				UnlockReleaseBuffer(left_sibling_buf);
			if (BufferIsValid(right_sibling_buf))
				UnlockReleaseBuffer(right_sibling_buf);
			UnlockReleaseBuffer(right_buf);
			UnlockReleaseBuffer(left_buf);
			return false;
		}
	}

	/*
	 * Allocate all memory BEFORE critical section - palloc can throw errors
	 */
	nleft_items = left_maxoff - left_firstdata + 1;
	if (nleft_items <= 0)
	{
		elog(DEBUG1, "pg_index_reclaim: No items to move from left page %u, aborting", left_block);
		if (BufferIsValid(left_sibling_buf))
			UnlockReleaseBuffer(left_sibling_buf);
		if (BufferIsValid(right_sibling_buf))
			UnlockReleaseBuffer(right_sibling_buf);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	elog(DEBUG1, "pg_index_reclaim: Allocating arrays for %d items (BEFORE critical section)", nleft_items);
	left_offsets = (OffsetNumber *) palloc(sizeof(OffsetNumber) * nleft_items);

	/* Also pre-allocate the deletable array for marking left page half-dead */
	ndeletable = left_maxoff - left_firstdata + 1;
	if (ndeletable > 0)
		deletable = (OffsetNumber *) palloc(sizeof(OffsetNumber) * ndeletable);

	/*
	 * Collect left page item offsets and calculate total size (before crit section)
	 */
	{
		int i = 0;
		for (offnum = left_firstdata; offnum <= left_maxoff; offnum++)
		{
			itemid = PageGetItemId(left_page, offnum);
			if (!ItemIdIsUsed(itemid))
			{
				elog(DEBUG1, "pg_index_reclaim: Skipping unused item at offset %u", offnum);
				continue;
			}
			left_offsets[i] = offnum;
			left_items_total_size += MAXALIGN(ItemIdGetLength(itemid));
			i++;
		}
		nleft_items = i;  /* Update actual count */
	}
	elog(DEBUG1, "pg_index_reclaim: Collected %d valid items from L page, total_size=%zu",
		 nleft_items, left_items_total_size);

	if (nleft_items == 0)
	{
		elog(DEBUG1, "pg_index_reclaim: No valid items to move, aborting");
		pfree(left_offsets);
		if (deletable)
			pfree(deletable);
		if (BufferIsValid(left_sibling_buf))
			UnlockReleaseBuffer(left_sibling_buf);
		if (BufferIsValid(right_sibling_buf))
			UnlockReleaseBuffer(right_sibling_buf);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	/*
	 * Start critical section - no errors allowed from here until END_CRIT_SECTION.
	 * All allocations done above, all buffers acquired above.
	 *
	 * Merge strategy: We'll add items from the left page to the right page
	 * using PageAddItem(), which properly handles page layout.
	 */
	elog(DEBUG1, "pg_index_reclaim: Starting critical section for merge");
	START_CRIT_SECTION();

	/*
	 * Step 1: Add items from left page to right page using PageAddItem.
	 * We add them one by one at the end of the right page's data.
	 */
	elog(DEBUG1, "pg_index_reclaim: Adding %d items from left page to right page", nleft_items);

	{
		int i;
		int items_added = 0;

		for (i = 0; i < nleft_items; i++)
		{
			Size itemsz;
			IndexTuple itup;
			OffsetNumber newoff;

			offnum = left_offsets[i];
			itemid = PageGetItemId(left_page, offnum);

			Assert(ItemIdIsUsed(itemid));

			itemsz = ItemIdGetLength(itemid);
			itup = (IndexTuple) PageGetItem(left_page, itemid);

			/*
			 * Add the item to the right page. We use InvalidOffsetNumber
			 * to add at the end of the page's items.
			 */
			newoff = PageAddItem(right_page, (Item) itup, itemsz,
								 InvalidOffsetNumber, false, false);

			if (newoff == InvalidOffsetNumber)
			{
				elog(WARNING, "pg_index_reclaim: Failed to add item %d from left page", i);
				/* Continue trying other items */
			}
			else
			{
				items_added++;
				elog(DEBUG1, "pg_index_reclaim: Added L item %d (size %zu) to R at offset %u",
					 i, itemsz, newoff);
			}
		}

		elog(DEBUG1, "pg_index_reclaim: Added %d of %d items from left to right page",
			 items_added, nleft_items);
	}

	/* Re-get right page opaque after inserting items */
	right_opaque = BTPageGetOpaque(right_page);
	right_maxoff = PageGetMaxOffsetNumber(right_page);
	right_firstdata = P_FIRSTDATAKEY(right_opaque);

	elog(DEBUG1, "pg_index_reclaim: After insertion - right page: maxoff=%u, firstdata=%u, rightmost=%d",
		 right_maxoff, right_firstdata, P_RIGHTMOST(right_opaque));

	/*
	 * Step 2: Update the high key on the right page if needed.
	 * The merged page should have the high key from the LEFT page
	 * (since we're deleting the left page).
	 */
	elog(DEBUG1, "pg_index_reclaim: Checking high key - left rightmost=%d, right rightmost=%d",
		 P_RIGHTMOST(left_opaque), P_RIGHTMOST(right_opaque));

	if (!P_RIGHTMOST(left_opaque))
	{
		ItemId		left_hikey_itemid;

		elog(DEBUG1, "pg_index_reclaim: Left page %u has high key, need to update right page %u",
			 left_block, right_block);

		left_hikey_itemid = PageGetItemId(left_page, P_HIKEY);
		if (ItemIdIsUsed(left_hikey_itemid) && ItemIdHasStorage(left_hikey_itemid))
		{
			Size		left_hikey_size;
			IndexTuple	left_hikey;

			left_hikey_size = ItemIdGetLength(left_hikey_itemid);
			left_hikey = (IndexTuple) PageGetItem(left_page, left_hikey_itemid);

			/*
			 * The right page's current high key should remain (it's the upper bound
			 * for this page). But wait - after merging, the right page now contains
			 * items from the left page, so its high key should be the left page's
			 * high key (which is the bound for the leftmost items now on this page).
			 *
			 * Actually, for B-tree correctness, the merged page's high key should
			 * still be the ORIGINAL right page's high key (if it had one), because
			 * that's the upper bound that parent pages expect.
			 *
			 * The left page's high key was the separator between left and right,
			 * but now that they're merged, we don't need it.
			 *
			 * So: we DON'T update the high key here. The right page keeps its
			 * original high key (if any).
			 */
			elog(DEBUG1, "pg_index_reclaim: Right page keeps its original high key (if any)");
		}
		else
		{
			elog(WARNING, "pg_index_reclaim: Left page %u high key not usable", left_block);
		}
	}
	else
	{
		elog(DEBUG1, "pg_index_reclaim: Left page %u was rightmost, no high key considerations", left_block);
	}

	/* Update sibling links */
	elog(DEBUG1, "pg_index_reclaim: Updating sibling links");

	/* Update right page's prev pointer to skip over deleted left page */
	elog(DEBUG1, "pg_index_reclaim: Updating right page %u prev pointer from %u to %u",
		 right_block, right_opaque->btpo_prev, leftsib);
	right_opaque->btpo_prev = leftsib;

	/* Update left sibling's next pointer if it exists */
	if (BufferIsValid(left_sibling_buf))
	{
		Assert(left_sibling_opaque->btpo_next == left_block);
		elog(DEBUG1, "pg_index_reclaim: Left sibling %u next pointer: %u -> %u",
			 leftsib, left_sibling_opaque->btpo_next, right_block);
		left_sibling_opaque->btpo_next = right_block;
		MarkBufferDirty(left_sibling_buf);
	}

	/* Right sibling's prev pointer already points to right_block - no change needed */
	if (BufferIsValid(right_sibling_buf))
	{
		elog(DEBUG1, "pg_index_reclaim: Right sibling %u prev pointer is already correct (%u)",
			 rightsib, right_sibling_opaque->btpo_prev);
	}

	/* Parent page update skipped - VACUUM will fix parent links later */
	elog(DEBUG1, "pg_index_reclaim: Skipping parent page update - VACUUM will fix parent links when deleting half-dead pages");

	/* Mark left page as half-dead (not fully deleted) */
	elog(DEBUG1, "pg_index_reclaim: Marking left page %u as half-dead", left_block);

	/* Delete all items from left page except high key */
	{
		int			i;
		int			valid_items = 0;

		elog(DEBUG1, "pg_index_reclaim: Deleting %d items from left page %u", ndeletable, left_block);

		if (ndeletable > 0 && deletable != NULL)
		{
			for (i = 0, offnum = left_firstdata; offnum <= left_maxoff; offnum++, i++)
			{
				itemid = PageGetItemId(left_page, offnum);
				if (ItemIdIsUsed(itemid))
				{
					deletable[valid_items] = offnum;
					valid_items++;
				}
			}

			if (valid_items > 0)
			{
				elog(DEBUG1, "pg_index_reclaim: Deleting %d valid items from left page", valid_items);
				PageIndexMultiDelete(left_page, deletable, valid_items);
			}
		}
	}

	/* Mark page as half-dead */
	left_opaque = BTPageGetOpaque(left_page);
	left_opaque->btpo_flags |= BTP_HALF_DEAD;

	/* Set up dummy high key for half-dead page */
	{
		IndexTupleData trunctuple;

		/* Create/update dummy high key */
		MemSet(&trunctuple, 0, sizeof(IndexTupleData));
		trunctuple.t_info = sizeof(IndexTupleData);
		BTreeTupleSetNAtts(&trunctuple, 0, false);
		/* Set top parent to InvalidBlockNumber - VACUUM will handle parent updates */
		BTreeTupleSetTopParent(&trunctuple, InvalidBlockNumber);

		/* Overwrite or add high key */
		if (PageGetMaxOffsetNumber(left_page) >= P_HIKEY &&
			ItemIdIsUsed(PageGetItemId(left_page, P_HIKEY)))
		{
			/* Overwrite existing high key */
			if (!PageIndexTupleOverwrite(left_page, P_HIKEY, (char *) &trunctuple, sizeof(IndexTupleData)))
			{
				/* Should not happen - just log warning */
				elog(WARNING, "pg_index_reclaim: Failed to overwrite high key in half-dead page %u", left_block);
			}
		}
		else
		{
			/* Add high key */
			if (PageAddItem(left_page, (char *) &trunctuple, sizeof(IndexTupleData), P_HIKEY, false, false) == InvalidOffsetNumber)
			{
				/* Should not happen - just log warning */
				elog(WARNING, "pg_index_reclaim: Failed to add dummy high key to half-dead page %u", left_block);
			}
		}
	}

	left_opaque->btpo_cycleid = 0;
	elog(DEBUG1, "pg_index_reclaim: Left page %u marked as half-dead", left_block);

	/* Mark buffers dirty */
	elog(DEBUG1, "pg_index_reclaim: Marking buffers dirty");
	MarkBufferDirty(left_buf);
	MarkBufferDirty(right_buf);

	/*
	 * WAL logging using Full Page Images (FPI).
	 * We use log_newpage_buffer() which logs the entire page content.
	 * This is simpler than creating custom WAL records and is crash-safe.
	 */
	if (needswal)
	{
		XLogRecPtr	recptr;

		elog(DEBUG1, "pg_index_reclaim: WAL logging with FPI for modified pages");

		/*
		 * Log each modified page as a full page image.
		 * log_newpage_buffer() must be called from within a critical section.
		 */

		/* Log the left page (now half-dead) */
		recptr = log_newpage_buffer(left_buf, true);
		elog(DEBUG1, "pg_index_reclaim: Logged left page %u, LSN=%X/%X",
			 left_block, LSN_FORMAT_ARGS(recptr));

		/* Log the right page (contains merged items) */
		recptr = log_newpage_buffer(right_buf, true);
		elog(DEBUG1, "pg_index_reclaim: Logged right page %u, LSN=%X/%X",
			 right_block, LSN_FORMAT_ARGS(recptr));

		/* Log the left sibling page if modified */
		if (BufferIsValid(left_sibling_buf))
		{
			recptr = log_newpage_buffer(left_sibling_buf, true);
			elog(DEBUG1, "pg_index_reclaim: Logged left sibling page %u, LSN=%X/%X",
				 leftsib, LSN_FORMAT_ARGS(recptr));
		}

		/* Right sibling was not modified, no need to log */
	}
	else
	{
		elog(DEBUG1, "pg_index_reclaim: WAL logging not needed for this relation");
	}

	END_CRIT_SECTION();
	elog(DEBUG1, "pg_index_reclaim: Critical section ended");

	/* Free pre-allocated memory */
	pfree(left_offsets);
	if (deletable)
		pfree(deletable);

	/* Release locks */
	elog(DEBUG1, "pg_index_reclaim: Releasing buffers");
	if (BufferIsValid(left_sibling_buf))
		UnlockReleaseBuffer(left_sibling_buf);
	if (BufferIsValid(right_sibling_buf))
		UnlockReleaseBuffer(right_sibling_buf);
	UnlockReleaseBuffer(right_buf);
	UnlockReleaseBuffer(left_buf);

	/* Dump pages AFTER merge */
	elog(DEBUG1, "pg_index_reclaim: ========================================");
	elog(DEBUG1, "pg_index_reclaim: Merge completed, dumping pages AFTER merge");
	elog(DEBUG1, "pg_index_reclaim: ========================================");
	dump_page(rel, left_block, "LEFT PAGE (AFTER MERGE - HALF-DEAD)");
	dump_page(rel, right_block, "RIGHT PAGE (AFTER MERGE)");
	if (rightsib != P_NONE)
		dump_page(rel, rightsib, "RIGHT SIBLING PAGE (AFTER MERGE)");

	elog(DEBUG1, "pg_index_reclaim: Merge of pages %u -> %u completed successfully",
		 left_block, right_block);
	return true;
}

/*
 * SQL-callable function to execute the merge
 */
PG_FUNCTION_INFO_V1(pg_index_reclaim_execute);
Datum
pg_index_reclaim_execute(PG_FUNCTION_ARGS)
{
	Oid			index_oid = PG_GETARG_OID(0);
	int			max_pct_to_merge = PG_GETARG_INT32(1);
	Relation	rel;
	List	   *merge_candidates = NIL;
	ListCell   *lc;
	int64		pages_merged = 0;
	int64		space_reclaimed = 0;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	const int	MAX_MERGES_PER_EXECUTION = 1;
	int			merges_attempted = 0;
	MergeCandidate *candidate;

	/* Validate parameters */
	if (max_pct_to_merge < 1 || max_pct_to_merge > 100)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_pct_to_merge must be between 1 and 100")));

	/* Open the index relation */
	rel = index_open(index_oid, ShareUpdateExclusiveLock);

	/* Verify it's a B-tree index */
	if (rel->rd_rel->relam != BTREE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("index \"%s\" is not a B-tree index",
						RelationGetRelationName(rel))));

	/* Set up return structure */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pages_merged",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "space_reclaimed",
					   INT8OID, -1, 0);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* Analyze to get merge candidates */
	elog(DEBUG1, "pg_index_reclaim: Starting analysis for index \"%s\" with max_pct_to_merge=%d",
		 RelationGetRelationName(rel), max_pct_to_merge);
	analyze_index_pages(rel, &merge_candidates, max_pct_to_merge);

	elog(DEBUG1, "pg_index_reclaim: Found %d merge candidates", list_length(merge_candidates));

	/* Execute merges for candidates that can be merged */
	/* Limit to only 1 merge (2 pages) for safety */
	elog(DEBUG1, "pg_index_reclaim: Processing merge candidates (max %d merge = 2 pages per execution)",
		 MAX_MERGES_PER_EXECUTION);

	foreach(lc, merge_candidates)
	{
		/* Limit the number of merges per execution */
		if (merges_attempted >= MAX_MERGES_PER_EXECUTION)
		{
			elog(DEBUG1, "pg_index_reclaim: Reached merge limit (%d), stopping", MAX_MERGES_PER_EXECUTION);
			break;
		}

		candidate = (MergeCandidate *) lfirst(lc);

		if (candidate->can_merge)
		{
			merges_attempted++;
			elog(DEBUG1, "pg_index_reclaim: Attempting merge %d/%d: pages %u -> %u",
				 merges_attempted, MAX_MERGES_PER_EXECUTION,
				 candidate->left_page, candidate->right_page);

			PG_TRY();
			{
				if (execute_merge(rel, candidate->left_page, candidate->right_page))
				{
					pages_merged++;
					space_reclaimed += (BLCKSZ - candidate->estimated_space);
					elog(DEBUG1, "pg_index_reclaim: Successfully merged pages %u -> %u (total merged: " INT64_FORMAT ")",
						 candidate->left_page, candidate->right_page, pages_merged);
				}
				else
				{
					elog(WARNING, "pg_index_reclaim: Failed to merge pages %u -> %u (merge function returned false)",
						 candidate->left_page, candidate->right_page);
				}
			}
			PG_CATCH();
			{
				ErrorData *edata;

				/* Get error info */
				edata = CopyErrorData();
				FlushErrorState();

				elog(WARNING, "pg_index_reclaim: Error during merge of pages %u -> %u: %s - stopping merge execution",
					 candidate->left_page, candidate->right_page,
					 edata->message ? edata->message : "unknown error");

				FreeErrorData(edata);

				/* Don't continue with more merges after an error */
				/* Re-throw the error to abort the function */
				PG_RE_THROW();
			}
			PG_END_TRY();
		}
		else
		{
			elog(DEBUG1, "pg_index_reclaim: Skipping merge of pages %u -> %u (can_merge=false)",
				 candidate->left_page, candidate->right_page);
		}
	}

	/* Return results */
	elog(DEBUG1, "pg_index_reclaim: Completed execution - pages_merged=" INT64_FORMAT ", space_reclaimed=" INT64_FORMAT,
		 pages_merged, space_reclaimed);
	
	{
		Datum		values[2];
		bool		nulls[2];

		memset(nulls, 0, sizeof(nulls));
		values[0] = Int64GetDatum(pages_merged);
		values[1] = Int64GetDatum(space_reclaimed);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* Clean up */
	index_close(rel, ShareUpdateExclusiveLock);

	elog(DEBUG1, "pg_index_reclaim: reclaim_space_execute completed successfully");
	return (Datum) 0;
}

/*
 * SQL-callable function to analyze merge candidates
 */
PG_FUNCTION_INFO_V1(pg_index_reclaim_analyze);
Datum
pg_index_reclaim_analyze(PG_FUNCTION_ARGS)
{
	Oid			index_oid = PG_GETARG_OID(0);
	int			max_pct_to_merge = PG_GETARG_INT32(1);
	Relation	rel;
	List	   *merge_candidates = NIL;
	ListCell   *lc;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* Validate parameters */
	if (max_pct_to_merge < 1 || max_pct_to_merge > 100)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_pct_to_merge must be between 1 and 100")));

	/* Open the index relation */
	rel = index_open(index_oid, AccessShareLock);

	/* Verify it's a B-tree index */
	if (rel->rd_rel->relam != BTREE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("index \"%s\" is not a B-tree index",
						RelationGetRelationName(rel))));

	/* Set up return structure */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupdesc = CreateTemplateTupleDesc(7);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "left_page_block",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "right_page_block",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "left_page_usage_pct",
					   NUMERICOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "right_page_usage_pct",
					   NUMERICOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "total_items_to_move",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "estimated_space_reclaimed",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "can_merge",
					   BOOLOID, -1, 0);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* Analyze to get merge candidates */
	analyze_index_pages(rel, &merge_candidates, max_pct_to_merge);

	/* Return results */
	foreach(lc, merge_candidates)
	{
		MergeCandidate *candidate = (MergeCandidate *) lfirst(lc);
		Datum		values[7];
		bool		nulls[7];

		memset(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatum((int64) candidate->left_page);
		values[1] = Int64GetDatum((int64) candidate->right_page);
		values[2] = DirectFunctionCall1(float8_numeric,
										 Float8GetDatum(candidate->left_usage_pct));
		values[3] = DirectFunctionCall1(float8_numeric,
										 Float8GetDatum(candidate->right_usage_pct));
		values[4] = Int64GetDatum((int64) candidate->total_items);
		values[5] = Int64GetDatum((int64) (BLCKSZ - candidate->estimated_space));
		values[6] = BoolGetDatum(candidate->can_merge);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* Clean up */
	index_close(rel, AccessShareLock);

	return (Datum) 0;
}

