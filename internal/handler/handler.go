package handler

import (
	"strconv"
	"sync"
	"time"

	"github.com/Avirat2211/blueis/internal/resp"
	"github.com/wangjia184/sortedset"
)

var Handlers = map[string]func([]resp.Value) resp.Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetAll,
	"COMMAND": command,
	"EXPIRE":  expire,
	"TTL":     ttl,
	"ZADD":    zadd,
	"ZRANGE":  zrange,
	"ZREM":    zrem,
}

func ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}
	return resp.Value{Typ: "string", Str: args[0].Bulk}
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

func set(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for SET command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	SETsMutex.Lock()
	SETs[key] = value
	SETsMutex.Unlock()

	return resp.Value{Typ: "string", Str: "OK"}
}

func get(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for GET command"}
	}

	key := args[0].Bulk

	cleanupIfExpired(key)

	SETsMutex.RLock()
	value, ok := SETs[key]
	SETsMutex.RUnlock()

	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "Bulk", Bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMutex = sync.RWMutex{}

func hset(args []resp.Value) resp.Value {

	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for HSET command"}
	}
	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	HSETsMutex.Lock()
	defer HSETsMutex.Unlock()

	_, ok := HSETs[hash]
	if !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value

	return resp.Value{Typ: "string", Str: "OK"}
}

func hget(args []resp.Value) resp.Value {

	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for HGET command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	cleanupIfExpired(hash)

	HSETsMutex.RLock()
	defer HSETsMutex.RUnlock()

	m, ok := HSETs[hash]
	if !ok {
		return resp.Value{Typ: "null"}
	}

	value, ok := m[key]
	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "Bulk", Bulk: value}
}

func hgetAll(args []resp.Value) resp.Value {

	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for HGETALL command"}
	}

	hash := args[0].Bulk

	cleanupIfExpired(hash)

	HSETsMutex.RLock()
	defer HSETsMutex.RUnlock()

	val, ok := HSETs[hash]
	if !ok {
		return resp.Value{Typ: "Array", Array: []resp.Value{}}
	}

	var value []resp.Value
	for x, y := range val {
		value = append(value, resp.Value{Typ: "Bulk", Bulk: x})
		value = append(value, resp.Value{Typ: "Bulk", Bulk: y})
	}

	return resp.Value{Typ: "Array", Array: value}
}

func command(args []resp.Value) resp.Value {
	return resp.Value{Typ: "Array", Array: []resp.Value{}}
}

var Expiry = map[string]int64{}
var ExpiryMutex = sync.RWMutex{}

func expire(args []resp.Value) resp.Value {

	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for EXPIRE command"}
	}

	key := args[0].Bulk

	seconds, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return resp.Value{Typ: "error", Str: "Invalid seconds value for EXPIRE command"}
	}

	SETsMutex.RLock()
	_, setOk := SETs[key]
	SETsMutex.RUnlock()

	HSETsMutex.RLock()
	_, hsetOk := HSETs[key]
	HSETsMutex.RUnlock()

	if !setOk && !hsetOk {
		return resp.Value{Typ: "string", Str: "0"}
	}

	expiryTime := time.Now().Unix() + int64(seconds)

	ExpiryMutex.Lock()
	Expiry[key] = expiryTime
	ExpiryMutex.Unlock()

	return resp.Value{Typ: "string", Str: "1"}

}

func ttl(args []resp.Value) resp.Value {

	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for TTL command"}
	}

	key := args[0].Bulk

	ExpiryMutex.RLock()
	exp, ok := Expiry[key]
	ExpiryMutex.RUnlock()

	if !ok {
		return resp.Value{Typ: "string", Str: "-1"}
	}

	ttl := exp - time.Now().Unix()

	if ttl < 0 {
		return resp.Value{Typ: "string", Str: "-2"}
	}

	return resp.Value{Typ: "string", Str: strconv.Itoa(int(ttl))}
}

func isExpired(key string) bool {

	ExpiryMutex.RLock()
	exp, ok := Expiry[key]
	ExpiryMutex.RUnlock()

	if !ok {
		return false
	}

	return time.Now().Unix() > exp
}

func cleanupIfExpired(key string) {

	if isExpired(key) {

		SETsMutex.Lock()
		delete(SETs, key)
		SETsMutex.Unlock()

		HSETsMutex.Lock()
		delete(HSETs, key)
		HSETsMutex.Unlock()

		ExpiryMutex.Lock()
		delete(Expiry, key)
		ExpiryMutex.Unlock()
	}
}

var ZSETs = map[string]*sortedset.SortedSet{}
var ZSETsMutex = sync.RWMutex{}

func zadd(args []resp.Value) resp.Value {

	if len(args) < 3 || len(args)%2 == 0 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for ZADD command"}
	}

	key := args[0].Bulk
	// fmt.Printf("[DEBUG] ZSETs has %d keys before %s on %q\n", len(ZSETs), "ZADD", key)
	ZSETsMutex.Lock()
	defer ZSETsMutex.Unlock()
	ss, ok := ZSETs[key]
	if !ok {
		ss = sortedset.New()
		ZSETs[key] = ss
	}

	count := 0

	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseInt(args[i].Bulk, 10, 64)
		if err != nil {
			return resp.Value{Typ: "error", Str: "Invalid score for ZADD"}
		}
		member := args[i+1].Bulk
		check := ss.GetByKey(member)
		// fmt.Println("ZADD", key, score, member)
		// fmt.Println("Check:", check)
		ss.AddOrUpdate(member, sortedset.SCORE(score), nil)
		if check == nil {
			// not found
			count++
		}
	}

	return resp.Value{Typ: "string", Str: strconv.Itoa(count)}

}

func zrange(args []resp.Value) resp.Value {

	if len(args) < 3 || len(args) > 4 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for ZRANGE command"}
	}
	key := args[0].Bulk
	// fmt.Printf("[DEBUG] ZSETs has %d keys before %s on %q\n", len(ZSETs), "ZRANGE", key)
	l, err1 := strconv.Atoi(args[1].Bulk)
	r, err2 := strconv.Atoi(args[2].Bulk)
	withscores := false
	if err1 != nil || err2 != nil {
		return resp.Value{Typ: "error", Str: "Invalid range for ZRANGE command"}
	}
	if len(args) == 4 {
		withscores = true
	}
	ZSETsMutex.RLock()
	ss, ok := ZSETs[key]
	ZSETsMutex.RUnlock()
	if !ok {
		return resp.Value{Typ: "Array", Array: []resp.Value{}}
	}
	size := ss.GetCount()
	// fmt.Println("Key:", key)
	// fmt.Println("size:", size)
	if l < 0 {
		l = size + l
	}
	if r < 0 {
		r = size + r
	}
	if r >= size {
		r = size - 1
	}
	if l > r || l >= size {
		return resp.Value{Typ: "Array", Array: []resp.Value{}}
	}
	elements := ss.GetByRankRange(l+1, r+1, false)
	var ans []resp.Value
	for _, itp := range elements {
		it := *(itp)
		ans = append(ans, resp.Value{Typ: "Bulk", Bulk: it.Key()})
		if withscores {
			ans = append(ans, resp.Value{Typ: "Bulk", Bulk: strconv.FormatInt(int64(it.Score()), 10)})
		}
	}
	return resp.Value{Typ: "Array", Array: ans}
}

func zrem(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for ZREM command"}
	}
	count := 0
	key := args[0].Bulk
	ZSETsMutex.Lock()
	defer ZSETsMutex.Unlock()
	ss, ok := ZSETs[key]
	if !ok {
		return resp.Value{Typ: "string", Str: "0"}
	}
	for i := 1; i < len(args); i++ {
		val := args[i].Bulk
		if ss.Remove(val) != nil {
			count++
		}
	}
	return resp.Value{Typ: "string", Str: strconv.Itoa(count)}
}
