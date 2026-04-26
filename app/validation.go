package main

var minArgs = map[string]int{
	//access
	"PING": 1,
	"ECHO": 2,

	//string
	"GET": 2,
	"SET": 3,

	//list
	"RPUSH":  3,
	"LPUSH":  3,
	"LRANGE": 4,
	"LLEN":   2,
	"LPOP":   2,
	"BLPOP":  3,
	"INCR":   2,
}