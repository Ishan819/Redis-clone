// Package skiplist implements a skip list from scratch: an ordered,
// probabilistic linked structure that supports expected O(log n)
// insertion, deletion, and rank/range lookups. It's the ordered index
// backing this project's sorted-set (ZSET) commands, mirroring the
// score-plus-member skip list Redis itself uses internally, including
// per-level span counters so that rank and range queries don't require a
// linear scan.
//
// The algorithms here (insert, delete, rank, range-by-rank) follow the
// classic skip list design popularized by William Pugh and used by Redis's
// own t_zset.c: each node's height is chosen randomly, and higher levels
// let searches skip over large stretches of the list, giving O(log n)
// expected search depth without any rebalancing.
package skiplist

import (
	"math/rand"
)

const (
	// maxLevel bounds how many levels a node (and the list) can have. 32
	// levels comfortably supports lists far larger than this project will
	// ever hold (p=0.25 makes level 32 astronomically unlikely to be
	// needed below roughly 4^32 elements), matching Redis's own
	// ZSKIPLIST_MAXLEVEL.
	maxLevel = 32

	// p is the probability of a node growing an additional level, again
	// matching Redis's ZSKIPLIST_P. Lower p means fewer, more effective
	// higher levels and less memory overhead per node.
	p = 0.25
)

// Element is one (score, member) pair stored in a SkipList.
type Element struct {
	Score  float64
	Member string
}

// less reports whether a sorts strictly before b: primarily by Score
// ascending, with ties broken by Member ascending — matching Redis's
// sorted-set ordering.
func less(a, b Element) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	return a.Member < b.Member
}

// lessOrEqual reports whether a sorts before or exactly at b's position.
func lessOrEqual(a, b Element) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	return a.Member <= b.Member
}

// level is one rung of a node's forward-pointer ladder: the next node
// reachable at this height, and span — the number of level-0 (bottom
// level) hops between this node and that next node. Span is what makes
// Rank and Range possible in O(log n): summing spans along a search path
// gives a node's rank without walking the bottom level node by node.
type level struct {
	forward *node
	span    int
}

type node struct {
	element Element
	levels  []level // length == this node's height, 1..maxLevel
}

// SkipList is an ordered container of (score, member) Elements sorted by
// Score ascending, ties broken by Member ascending. It supports expected
// O(log n) Insert, Delete, and Rank, and expected O(log n + k) Range of k
// elements.
//
// SkipList does not enforce that each Member appears only once — it is a
// raw ordered multiset of (score, member) pairs, matching how Redis's own
// skip list is just one half of a sorted set (paired with a dict for
// member -> score lookups). Callers that need "one entry per member"
// semantics, such as this project's ZADD, must Delete any existing entry
// for a member (using its old score) before Inserting its new one.
//
// SkipList is not safe for concurrent use; callers needing that must
// supply their own locking (as this project's store package does).
type SkipList struct {
	head   *node
	level  int // number of levels currently in use, 1..maxLevel
	length int
}

// New returns an empty SkipList.
func New() *SkipList {
	return &SkipList{
		head:  &node{levels: make([]level, maxLevel)},
		level: 1,
	}
}

// Len returns the number of elements in the list.
func (sl *SkipList) Len() int {
	return sl.length
}

// randomLevel picks a node height using repeated coin flips with success
// probability p, capped at maxLevel — the standard skip list level
// distribution (P(level >= k) = p^(k-1)).
func randomLevel() int {
	lvl := 1
	for lvl < maxLevel && rand.Float64() < p {
		lvl++
	}
	return lvl
}

// Insert adds a new element with the given score and member.
//
// This follows the classic skip list insert algorithm: walk down from the
// top level, at each level advancing as far as possible while staying
// before the insertion point (recording both the predecessor node at that
// level and the cumulative span walked, in update/rank), then splice the
// new node in at every level up to its randomly chosen height, fixing up
// span counts along the way so they stay accurate for future Rank/Range
// calls.
func (sl *SkipList) Insert(score float64, member string) {
	target := Element{Score: score, Member: member}

	update := make([]*node, maxLevel)
	rank := make([]int, maxLevel)

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.levels[i].forward != nil && less(x.levels[i].forward.element, target) {
			rank[i] += x.levels[i].span
			x = x.levels[i].forward
		}
		update[i] = x
	}

	newLevel := randomLevel()
	if newLevel > sl.level {
		// Growing the list's height: the new top levels don't exist on
		// any node yet, so their "predecessor" is the head, and their
		// span (head to the end of the list) is simply the current
		// length — about to be overwritten below once we know exactly
		// where the new node sits.
		for i := sl.level; i < newLevel; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].levels[i].span = sl.length
		}
		sl.level = newLevel
	}

	n := &node{element: target, levels: make([]level, newLevel)}
	for i := 0; i < newLevel; i++ {
		n.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = n
		// rank[0]-rank[i] is how many level-0 hops separate update[i]
		// (the predecessor at level i) from update[0] (the predecessor at
		// level 0, i.e. n's immediate left neighbor) — the new node's
		// span must skip over that same distance beyond itself, and the
		// predecessor's span shrinks to reach exactly the new node.
		n.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = (rank[0] - rank[i]) + 1
	}
	// Levels above the new node's height are unaffected in structure, but
	// now span one extra node.
	for i := newLevel; i < sl.level; i++ {
		update[i].levels[i].span++
	}

	sl.length++
}

// Delete removes the element with the exact (score, member) pair, if
// present, and reports whether it was found and removed.
func (sl *SkipList) Delete(score float64, member string) bool {
	target := Element{Score: score, Member: member}
	update := make([]*node, maxLevel)

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && less(x.levels[i].forward.element, target) {
			x = x.levels[i].forward
		}
		update[i] = x
	}

	x = x.levels[0].forward
	if x == nil || x.element.Score != score || x.element.Member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].levels[i].forward == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].forward = x.levels[i].forward
		} else {
			update[i].levels[i].span--
		}
	}
	// Shrink the list's height if the top levels are now unused.
	for sl.level > 1 && sl.head.levels[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
	return true
}

// Rank returns the exact (score, member) element's 0-based rank in
// ascending order, and whether it was found.
func (sl *SkipList) Rank(score float64, member string) (int, bool) {
	target := Element{Score: score, Member: member}
	x := sl.head
	rank := 0
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && lessOrEqual(x.levels[i].forward.element, target) {
			rank += x.levels[i].span
			x = x.levels[i].forward
		}
		if x != sl.head && x.element.Score == score && x.element.Member == member {
			// rank counted the hop onto x itself, so it's currently
			// 1-based (the first element has rank 1); the public API is
			// 0-based.
			return rank - 1, true
		}
	}
	return 0, false
}

// Range returns the elements at ranks [start, stop], inclusive, in
// ascending order. Negative indices count from the end (-1 is the last
// element), matching Redis's ZRANGE index semantics; an empty or
// out-of-range result yields an empty (non-nil) slice rather than an
// error, and indices clamp rather than panic.
func (sl *SkipList) Range(start, stop int) []Element {
	n := sl.length
	if n == 0 {
		return []Element{}
	}
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if start >= n || start > stop {
		return []Element{}
	}
	if stop >= n {
		stop = n - 1
	}

	x := sl.elementAtRank(start)
	result := make([]Element, 0, stop-start+1)
	for x != nil && len(result) < stop-start+1 {
		result = append(result, x.element)
		x = x.levels[0].forward
	}
	return result
}

// elementAtRank returns the node at the given 0-based rank in O(log n)
// expected time by summing spans along a top-down search path, or nil if
// rank is out of bounds. This is the same technique Rank uses in reverse:
// there, we sum spans to find a target element's rank; here, we sum spans
// until they reach a target rank.
func (sl *SkipList) elementAtRank(rank int) *node {
	target := rank + 1 // internal traversal is 1-based (head is rank 0)
	x := sl.head
	traversed := 0
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && traversed+x.levels[i].span <= target {
			traversed += x.levels[i].span
			x = x.levels[i].forward
		}
		if traversed == target {
			return x
		}
	}
	return nil
}
