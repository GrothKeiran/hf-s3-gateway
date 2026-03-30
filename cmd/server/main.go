package main

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/GrothKeiran/hf-s3-gateway/internal/server"
)

func main() {
	addr := getenv("APP_ADDR", ":9000")
	r := gin.Default()
	server.RegisterRoutes(r)
	log.Printf("hf-s3-gateway listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatal(err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
