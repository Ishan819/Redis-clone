package skiplist

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestEmpty(t *testing.T) {
	sl := New()
	if n := sl.Len(); n != 0 {
		t.Errorf("Len() = %d, want 0", n)
	}
	if got := sl.Range(0, -1); !reflect.DeepEqual(got, []Element{}) {
		t.Errorf("Range(0, -1) on empty list = %v, want []", got)
	}
	if _, ok := sl.Rank(1, "x"); ok {
		t.Error("Rank on empty list ok = true, want false")
	}
	if sl.Delete(1, "x") {
		t.Error("Delete on empty list = true, want false")
	}
}

func TestInsertAndOrdering(t *testing.T) {
	sl := New()
	// Insert out of order; the list must still read back sorted.
	sl.Insert(3, "c")
	sl.Insert(1, "a")
	sl.Insert(2, "b")

	if n := sl.Len(); n != 3 {
		t.Fatalf("Len() = %d, want 3", n)
	}

	got := sl.Range(0, -1)
	want := []Element{{1, "a"}, {2, "b"}, {3, "c"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Range(0, -1) = %v, want %v", got, want)
	}
}

func TestTieBrokenByMember(t *testing.T) {
	sl := New()
	// Same score, different members: must order by member ascending.
	sl.Insert(5, "zebra")
	sl.Insert(5, "apple")
	sl.Insert(5, "mango")

	got := sl.Range(0, -1)
	want := []Element{{5, "apple"}, {5, "mango"}, {5, "zebra"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Range(0, -1) with tied scores = %v, want %v", got, want)
	}
}

func TestDelete(t *testing.T) {
	sl := New()
	sl.Insert(1, "a")
	sl.Insert(2, "b")
	sl.Insert(3, "c")

	if !sl.Delete(2, "b") {
		t.Fatal("Delete(2, b) = false, want true")
	}
	if n := sl.Len(); n != 2 {
		t.Errorf("Len() after delete = %d, want 2", n)
	}
	got := sl.Range(0, -1)
	want := []Element{{1, "a"}, {3, "c"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Range after delete = %v, want %v", got, want)
	}

	// Deleting again, or a nonexistent (score, member), reports false.
	if sl.Delete(2, "b") {
		t.Error("Delete(2, b) again = true, want false")
	}
	if sl.Delete(1, "nonexistent") {
		t.Error("Delete(1, nonexistent) = true, want false")
	}
	// Same member, wrong score also doesn't match — SkipList is a raw
	// (score, member) container with no member-uniqueness of its own.
	if sl.Delete(999, "a") {
		t.Error("Delete(999, a) = true, want false (wrong score)")
	}
}

func TestRank(t *testing.T) {
	sl := New()
	members := []Element{{10, "a"}, {20, "b"}, {30, "c"}, {40, "d"}, {50, "e"}}
	for _, e := range members {
		sl.Insert(e.Score, e.Member)
	}

	for i, e := range members {
		rank, ok := sl.Rank(e.Score, e.Member)
		if !ok || rank != i {
			t.Errorf("Rank(%v, %q) = %d, %v, want %d, true", e.Score, e.Member, rank, ok, i)
		}
	}

	if _, ok := sl.Rank(999, "nonexistent"); ok {
		t.Error("Rank(nonexistent) ok = true, want false")
	}
}

func TestRangeNegativeIndices(t *testing.T) {
	sl := New()
	for i, m := range []string{"a", "b", "c", "d", "e"} {
		sl.Insert(float64(i), m)
	}

	tests := []struct {
		name        string
		start, stop int
		want        []string
	}{
		{"whole list", 0, -1, []string{"a", "b", "c", "d", "e"}},
		{"last element", -1, -1, []string{"e"}},
		{"last three", -3, -1, []string{"c", "d", "e"}},
		{"middle slice", 1, 3, []string{"b", "c", "d"}},
		{"stop past end clamps", 0, 100, []string{"a", "b", "c", "d", "e"}},
		{"start past end is empty", 10, -1, []string{}},
		{"start after stop is empty", 3, 1, []string{}},
		{"very negative start clamps to 0", -100, 1, []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sl.Range(tt.start, tt.stop)
			gotMembers := make([]string, len(got))
			for i, e := range got {
				gotMembers[i] = e.Member
			}
			if !reflect.DeepEqual(gotMembers, tt.want) {
				t.Errorf("Range(%d, %d) members = %v, want %v", tt.start, tt.stop, gotMembers, tt.want)
			}
		})
	}
}

func TestManyLevelsGrowShrink(t *testing.T) {
	// Insert enough elements that some nodes almost certainly reach
	// several levels, then delete them all and confirm the list is left
	// in a consistent, empty state (exercising the level-shrinking path
	// in Delete).
	sl := New()
	const n = 2000
	for i := 0; i < n; i++ {
		sl.Insert(float64(i), memberName(i))
	}
	if got := sl.Len(); got != n {
		t.Fatalf("Len() after %d inserts = %d, want %d", n, got, n)
	}

	for i := 0; i < n; i++ {
		if !sl.Delete(float64(i), memberName(i)) {
			t.Fatalf("Delete(%d) = false, want true", i)
		}
	}
	if got := sl.Len(); got != 0 {
		t.Errorf("Len() after deleting everything = %d, want 0", got)
	}
	if got := sl.Range(0, -1); !reflect.DeepEqual(got, []Element{}) {
		t.Errorf("Range after deleting everything = %v, want []", got)
	}
}

// memberName gives every int a distinct, deterministically-ordered-looking
// string name, so tests can build many (score, member) pairs without
// worrying about member collisions.
func memberName(i int) string {
	return fmt.Sprintf("m%04d", i)
}

// TestRandomizedCrossCheck drives the SkipList with a long random sequence
// of inserts and deletes while maintaining a plain sorted slice as a
// reference model, and after every operation verifies that the SkipList's
// full ordering (via Range), every element's Rank, and the list's Len all
// agree exactly with the reference. This is the primary correctness net
// for the span/rank bookkeeping, which is easy to get subtly wrong.
func TestRandomizedCrossCheck(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sl := New()
	var reference []Element // kept sorted by (Score, Member), no duplicates

	refLess := func(a, b Element) bool {
		if a.Score != b.Score {
			return a.Score < b.Score
		}
		return a.Member < b.Member
	}
	refIndex := func(e Element) int {
		return sort.Search(len(reference), func(i int) bool {
			return !refLess(reference[i], e)
		})
	}
	refContains := func(e Element) bool {
		i := refIndex(e)
		return i < len(reference) && reference[i] == e
	}
	refInsert := func(e Element) {
		i := refIndex(e)
		reference = append(reference, Element{})
		copy(reference[i+1:], reference[i:])
		reference[i] = e
	}
	refDelete := func(e Element) {
		i := refIndex(e)
		reference = append(reference[:i], reference[i+1:]...)
	}
	verify := func() {
		t.Helper()
		if got := sl.Len(); got != len(reference) {
			t.Fatalf("Len() = %d, want %d (reference: %v)", got, len(reference), reference)
		}
		got := sl.Range(0, -1)
		if len(got) != len(reference) {
			t.Fatalf("Range(0, -1) length = %d, want %d", len(got), len(reference))
		}
		for i, e := range reference {
			if got[i] != e {
				t.Fatalf("Range(0, -1)[%d] = %v, want %v (full got=%v, want=%v)", i, got[i], e, got, reference)
			}
			rank, ok := sl.Rank(e.Score, e.Member)
			if !ok || rank != i {
				t.Fatalf("Rank(%v) = %d, %v, want %d, true", e, rank, ok, i)
			}
		}
	}

	const ops = 3000
	const scoreSpace = 50 // small range to force lots of tied scores
	const memberSpace = 200

	for op := 0; op < ops; op++ {
		e := Element{
			Score:  float64(rng.Intn(scoreSpace)),
			Member: memberName(rng.Intn(memberSpace)),
		}
		if refContains(e) {
			// Member+score pair already present: delete it (also
			// exercises Delete on an existing element).
			if !sl.Delete(e.Score, e.Member) {
				t.Fatalf("op %d: Delete(%v) = false, want true", op, e)
			}
			refDelete(e)
		} else {
			sl.Insert(e.Score, e.Member)
			refInsert(e)
		}
		verify()
	}

	// Also spot-check a handful of arbitrary sub-ranges against the
	// reference slice.
	for i := 0; i < 20; i++ {
		start := rng.Intn(len(reference) + 1)
		length := rng.Intn(10)
		stop := start + length
		got := sl.Range(start, stop)
		wantEnd := stop + 1
		if wantEnd > len(reference) {
			wantEnd = len(reference)
		}
		var want []Element
		if start < len(reference) {
			want = append(want, reference[start:wantEnd]...)
		}
		if len(want) == 0 {
			want = []Element{}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Range(%d, %d) = %v, want %v", start, stop, got, want)
		}
	}
}
