// Command redis-server runs the redis-clone server.
package main

import (
	"log"

	"github.com/Ishan819/Redis-clone/internal/server"
)

const defaultAddr = ":6379"

func main() {
	srv := server.New(defaultAddr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
