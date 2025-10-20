package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/Avirat2211/blueis/internal/aof"
	"github.com/Avirat2211/blueis/internal/handler"
	"github.com/Avirat2211/blueis/internal/resp"
)

func main() {

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	aofInstance, err := aof.NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aofInstance.Close()
	aofInstance.Read(func(value resp.Value) {

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		if command == "EXPIRESAT" {
			if len(args) == 2 {
				key := args[0].Bulk
				timestamp, err := strconv.ParseInt(args[1].Bulk, 10, 64)
				if err == nil {
					handler.ExpiryMutex.Lock()
					handler.Expiry[key] = timestamp
					handler.ExpiryMutex.Unlock()
				}
			}
			return
		}

		handler, ok := handler.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)

	})

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("New connection from", conn.RemoteAddr())
		go handleConnection(conn, aofInstance)
	}
}

func handleConnection(conn net.Conn, aofInstance *aof.Aof) {
	defer conn.Close()

	for {
		respp := resp.NewResp(conn)
		value, err := respp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.Typ != "Array" {
			fmt.Println("Invalid request, expected Array")
			continue
		}

		if len(value.Array) == 0 {
			fmt.Println("Invalid request, expected Array length > 0")
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		writer := resp.NewWriter(conn)

		handler, ok := handler.Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(resp.Value{Typ: "string", Str: ""})
			continue
		}

		if command == "SET" || command == "HSET" || command == "ZADD" {
			err := aofInstance.Write(value)
			if err != nil {
				fmt.Println(err)
			}
		} else if command == "EXPIRE" {
			err := aof.HandleExpireWrite(aofInstance, args)
			if err != nil {
				fmt.Println(err)
			}
		}

		result := handler(args)
		writer.Write(result)
	}
}
