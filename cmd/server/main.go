package main

import (
	"fmt"
	"log"

	"github.com/peytonrunyan/proglog/internal/server"
)

const port string = "8082"

func main() {
	srv := server.NewHTTPServer(":" + port)
	fmt.Printf("Server running on port %s...", port)
	log.Fatal(srv.ListenAndServe())
}
