// Package command implements Redis commands. Each command is a Handler
// that takes the shared store plus its arguments (the command name itself
// excluded) and returns the RESP reply to send back to the client.
package command

import (
	"strconv"
	"strings"

	"github.com/Ishan819/Redis-clone/internal/resp"
	"github.com/Ishan819/Redis-clone/internal/store"
)

// Handler executes a single command against s and returns the RESP reply
// for it.
type Handler func(s *store.Store, args []string) resp.Value

// registry maps upper-cased command names to their handlers.
var registry = map[string]Handler{
	"PING": ping,
	"ECHO": echo,

	"SET":    set,
	"GET":    get,
	"DEL":    del,
	"EXISTS": exists,
	"INCR":   incr,
	"DECR":   decr,
	"APPEND": appendCmd,
	"STRLEN": strlen,

	"HSET":    hset,
	"HGET":    hget,
	"HDEL":    hdel,
	"HGETALL": hgetall,
	"HEXISTS": hexists,
	"HLEN":    hlen,

	"LPUSH":  lpush,
	"RPUSH":  rpush,
	"LPOP":   lpop,
	"RPOP":   rpop,
	"LRANGE": lrange,
	"LLEN":   llen,

	"ZADD":    zadd,
	"ZSCORE":  zscore,
	"ZRANK":   zrank,
	"ZRANGE":  zrange,
	"ZREM":    zrem,
	"ZINCRBY": zincrby,
}

// Lookup returns the handler registered for name, matched case-insensitively
// (as Redis commands are), or nil if name isn't a known command.
func Lookup(name string) Handler {
	return registry[strings.ToUpper(name)]
}

// ping implements PING: with no arguments it replies +PONG; with one
// argument it echoes that argument back, matching real Redis behavior.
func ping(_ *store.Store, args []string) resp.Value {
	switch len(args) {
	case 0:
		return resp.SimpleStringValue("PONG")
	case 1:
		return resp.BulkStringValue(args[0])
	default:
		return resp.ErrorValue("ERR wrong number of arguments for 'ping' command")
	}
}

// echo implements ECHO: it requires exactly one argument, which it returns
// unchanged as a bulk string.
func echo(_ *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'echo' command")
	}
	return resp.BulkStringValue(args[0])
}

// --- Strings ---

// set implements SET key value: it unconditionally stores value under key
// (overwriting any existing value, of any type) and replies +OK. (Optional
// SET flags like EX/NX are not implemented yet.)
func set(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'set' command")
	}
	s.Set(args[0], args[1])
	return resp.SimpleStringValue("OK")
}

// get implements GET key: it returns the stored value as a bulk string, or
// a null bulk string if key doesn't exist.
func get(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'get' command")
	}
	v, ok, err := s.Get(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkStringValue(v)
}

// del implements DEL key [key ...]: it removes each given key (of any
// type) and returns the number of keys actually removed.
func del(s *store.Store, args []string) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'del' command")
	}
	return resp.IntegerValue(int64(s.Del(args...)))
}

// exists implements EXISTS key [key ...]: it returns how many of the given
// keys are present (counting repeats), regardless of type.
func exists(s *store.Store, args []string) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'exists' command")
	}
	return resp.IntegerValue(int64(s.Exists(args...)))
}

// incr implements INCR key: it increments the integer value at key by 1
// (treating a missing key as 0) and returns the new value. It errors if the
// existing value isn't an integer or key holds a non-string type.
func incr(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'incr' command")
	}
	n, err := s.Incr(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

// decr implements DECR key: the same as incr but subtracting 1.
func decr(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'decr' command")
	}
	n, err := s.Decr(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

// appendCmd implements APPEND key value: it appends value to the existing
// string at key (treating a missing key as empty) and returns the length
// of the result. Named appendCmd since append is a Go builtin.
func appendCmd(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'append' command")
	}
	n, err := s.Append(args[0], args[1])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// strlen implements STRLEN key: it returns the length of the string stored
// at key, or 0 if key doesn't exist.
func strlen(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'strlen' command")
	}
	n, err := s.Strlen(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// --- Hashes ---

// hset implements HSET key field value [field value ...]: it sets each
// field to its value in the hash at key (creating the hash if needed) and
// returns how many fields were newly created.
func hset(s *store.Store, args []string) resp.Value {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hset' command")
	}
	n, err := s.HSet(args[0], args[1:]...)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// hget implements HGET key field: it returns the field's value as a bulk
// string, or a null bulk string if the key or field doesn't exist.
func hget(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hget' command")
	}
	v, ok, err := s.HGet(args[0], args[1])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkStringValue(v)
}

// hdel implements HDEL key field [field ...]: it removes each given field
// from the hash at key and returns how many fields were actually removed.
func hdel(s *store.Store, args []string) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hdel' command")
	}
	n, err := s.HDel(args[0], args[1:]...)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// hgetall implements HGETALL key: it returns all fields and values in the
// hash at key as a flat array (field1, value1, field2, value2, ...), or an
// empty array if key doesn't exist.
func hgetall(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hgetall' command")
	}
	fields, err := s.HGetAll(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	vals := make([]resp.Value, 0, len(fields)*2)
	for field, value := range fields {
		vals = append(vals, resp.BulkStringValue(field), resp.BulkStringValue(value))
	}
	return resp.ArrayValue(vals...)
}

// hexists implements HEXISTS key field: it returns 1 if field is present
// in the hash at key, else 0.
func hexists(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hexists' command")
	}
	ok, err := s.HExists(args[0], args[1])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	if ok {
		return resp.IntegerValue(1)
	}
	return resp.IntegerValue(0)
}

// hlen implements HLEN key: it returns the number of fields in the hash at
// key, or 0 if key doesn't exist.
func hlen(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hlen' command")
	}
	n, err := s.HLen(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// --- Lists ---

// lpush implements LPUSH key value [value ...]: it pushes each value onto
// the head of the list at key (creating the list if needed) and returns
// the list's new length.
func lpush(s *store.Store, args []string) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lpush' command")
	}
	n, err := s.LPush(args[0], args[1:]...)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// rpush implements RPUSH key value [value ...]: it pushes each value onto
// the tail of the list at key (creating the list if needed) and returns
// the list's new length.
func rpush(s *store.Store, args []string) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'rpush' command")
	}
	n, err := s.RPush(args[0], args[1:]...)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// lpop implements LPOP key: it removes and returns the first element of
// the list at key, or a null bulk string if key doesn't exist or the list
// is empty.
func lpop(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lpop' command")
	}
	v, ok, err := s.LPop(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkStringValue(v)
}

// rpop implements RPOP key: it removes and returns the last element of the
// list at key, or a null bulk string if key doesn't exist or the list is
// empty.
func rpop(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'rpop' command")
	}
	v, ok, err := s.RPop(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkStringValue(v)
}

// lrange implements LRANGE key start stop: it returns the elements of the
// list at key between start and stop, inclusive, supporting negative
// indices (-1 is the last element) as Redis does.
func lrange(s *store.Store, args []string) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lrange' command")
	}
	start, err1 := strconv.ParseInt(args[1], 10, 64)
	stop, err2 := strconv.ParseInt(args[2], 10, 64)
	if err1 != nil || err2 != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	elems, err := s.LRange(args[0], start, stop)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	vals := make([]resp.Value, len(elems))
	for i, v := range elems {
		vals[i] = resp.BulkStringValue(v)
	}
	return resp.ArrayValue(vals...)
}

// llen implements LLEN key: it returns the length of the list at key, or 0
// if key doesn't exist.
func llen(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'llen' command")
	}
	n, err := s.LLen(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// --- Sorted sets ---

// formatScore renders a sorted-set score the way Redis does: the shortest
// decimal string that round-trips to the same float64, with no forced
// trailing ".0" for whole numbers (Redis prints "5", not "5.0").
func formatScore(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
}

// zadd implements ZADD key score member [score member ...]: it sets each
// member to its score in the sorted set at key (creating the set if
// needed) and returns how many members were newly added.
func zadd(s *store.Store, args []string) resp.Value {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zadd' command")
	}
	pairs := make([]store.ZAddPair, 0, (len(args)-1)/2)
	for i := 1; i+1 < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return resp.ErrorValue("ERR value is not a valid float")
		}
		pairs = append(pairs, store.ZAddPair{Score: score, Member: args[i+1]})
	}
	n, err := s.ZAdd(args[0], pairs...)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// zscore implements ZSCORE key member: it returns member's score as a
// bulk string, or a null bulk string if the key or member doesn't exist.
func zscore(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zscore' command")
	}
	score, ok, err := s.ZScore(args[0], args[1])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkStringValue(formatScore(score))
}

// zrank implements ZRANK key member: it returns member's 0-based rank (by
// score ascending, ties broken by member name) as an integer, or a null
// bulk string if the key or member doesn't exist.
func zrank(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrank' command")
	}
	rank, ok, err := s.ZRank(args[0], args[1])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.IntegerValue(int64(rank))
}

// zrange implements ZRANGE key start stop: it returns the members of the
// sorted set at key between start and stop, inclusive, ordered by score
// ascending, supporting negative indices (-1 is the last element) as
// Redis does.
func zrange(s *store.Store, args []string) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrange' command")
	}
	start, err1 := strconv.ParseInt(args[1], 10, 64)
	stop, err2 := strconv.ParseInt(args[2], 10, 64)
	if err1 != nil || err2 != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	members, err := s.ZRange(args[0], start, stop)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	vals := make([]resp.Value, len(members))
	for i, m := range members {
		vals[i] = resp.BulkStringValue(m.Member)
	}
	return resp.ArrayValue(vals...)
}

// zrem implements ZREM key member [member ...]: it removes each given
// member from the sorted set at key and returns how many were actually
// removed.
func zrem(s *store.Store, args []string) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrem' command")
	}
	n, err := s.ZRem(args[0], args[1:]...)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

// zincrby implements ZINCRBY key increment member: it adds increment to
// member's score (creating the set and/or member with a starting score of
// 0 if needed) and returns the new score as a bulk string.
func zincrby(s *store.Store, args []string) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zincrby' command")
	}
	delta, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not a valid float")
	}
	newScore, err := s.ZIncrBy(args[0], delta, args[2])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.BulkStringValue(formatScore(newScore))
}
