package store

import (
	"math"
	"testing"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("k", "v")
	got, ok := s.Get("k")
	if !ok || got != "v" {
		t.Errorf("Get(k) = %q, %v, want %q, true", got, ok, "v")
	}

	s.Set("k", "v2")
	got, ok = s.Get("k")
	if !ok || got != "v2" {
		t.Errorf("Get(k) after overwrite = %q, %v, want %q, true", got, ok, "v2")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	if _, ok := s.Get("nope"); ok {
		t.Error("Get(missing) ok = true, want false")
	}
}

func TestDel(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	if n := s.Del("a", "b", "c"); n != 2 {
		t.Errorf("Del(a, b, c) = %d, want 2", n)
	}
	if _, ok := s.Get("a"); ok {
		t.Error("a still present after Del")
	}
	if n := s.Del("a"); n != 0 {
		t.Errorf("Del(already-deleted) = %d, want 0", n)
	}
}

func TestExists(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	if n := s.Exists("a", "b", "c"); n != 2 {
		t.Errorf("Exists(a, b, c) = %d, want 2", n)
	}
	// Redis counts a repeated key once per occurrence in the arg list.
	if n := s.Exists("a", "a"); n != 2 {
		t.Errorf("Exists(a, a) = %d, want 2", n)
	}
}

func TestIncrDecr(t *testing.T) {
	s := New()

	n, err := s.Incr("counter")
	if err != nil || n != 1 {
		t.Fatalf("Incr(missing key) = %d, %v, want 1, nil", n, err)
	}

	n, err = s.Incr("counter")
	if err != nil || n != 2 {
		t.Fatalf("Incr(counter) = %d, %v, want 2, nil", n, err)
	}

	n, err = s.Decr("counter")
	if err != nil || n != 1 {
		t.Fatalf("Decr(counter) = %d, %v, want 1, nil", n, err)
	}

	n, err = s.IncrBy("counter", 10)
	if err != nil || n != 11 {
		t.Fatalf("IncrBy(counter, 10) = %d, %v, want 11, nil", n, err)
	}
}

func TestIncrNonInteger(t *testing.T) {
	s := New()
	s.Set("k", "not a number")

	if _, err := s.Incr("k"); err == nil {
		t.Error("Incr(non-integer value) err = nil, want error")
	}
	if _, err := s.Decr("k"); err == nil {
		t.Error("Decr(non-integer value) err = nil, want error")
	}
}

func TestIncrOverflow(t *testing.T) {
	s := New()
	s.Set("k", "9223372036854775807") // math.MaxInt64

	if _, err := s.Incr("k"); err == nil {
		t.Error("Incr(MaxInt64) err = nil, want overflow error")
	}

	s.Set("k", "-9223372036854775808") // math.MinInt64
	if _, err := s.Decr("k"); err == nil {
		t.Error("Decr(MinInt64) err = nil, want overflow error")
	}

	s.Set("k", "0")
	if n, err := s.IncrBy("k", math.MaxInt64); err != nil || n != math.MaxInt64 {
		t.Errorf("IncrBy(0, MaxInt64) = %d, %v, want %d, nil", n, err, int64(math.MaxInt64))
	}
}

func TestAppend(t *testing.T) {
	s := New()

	n := s.Append("k", "hello")
	if n != 5 {
		t.Errorf("Append(missing key, hello) = %d, want 5", n)
	}

	n = s.Append("k", " world")
	if n != 11 {
		t.Errorf("Append(k, ' world') = %d, want 11", n)
	}

	got, _ := s.Get("k")
	if got != "hello world" {
		t.Errorf("Get(k) after appends = %q, want %q", got, "hello world")
	}
}

func TestStrlen(t *testing.T) {
	s := New()
	if n := s.Strlen("missing"); n != 0 {
		t.Errorf("Strlen(missing) = %d, want 0", n)
	}

	s.Set("k", "hello")
	if n := s.Strlen("k"); n != 5 {
		t.Errorf("Strlen(k) = %d, want 5", n)
	}
}

func TestConcurrentIncr(t *testing.T) {
	s := New()
	const goroutines = 50
	const perGoroutine = 100

	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < perGoroutine; j++ {
				if _, err := s.Incr("counter"); err != nil {
					t.Error(err)
				}
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	got, _ := s.Get("counter")
	want := "5000"
	if got != want {
		t.Errorf("counter after concurrent increments = %q, want %q", got, want)
	}
}
