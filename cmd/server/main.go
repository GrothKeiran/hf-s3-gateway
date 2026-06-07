package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/GrothKeiran/hf-s3-gateway/internal/server"
	"github.com/gin-gonic/gin"
)

func main() {
	addr := getenv("APP_ADDR", ":9000")
	r := gin.Default()
	server.RegisterRoutes(r)

	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: durationEnv("APP_READ_HEADER_TIMEOUT", 10*time.Second),
		IdleTimeout:       durationEnv("APP_IDLE_TIMEOUT", 120*time.Second),
		MaxHeaderBytes:    int(int64Env("APP_MAX_HEADER_BYTES", 1<<20)),
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("hf-s3-gateway listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), durationEnv("APP_SHUTDOWN_TIMEOUT", 20*time.Second))
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	case err := <-errCh:
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return fallback
	}
	return d
}

func int64Env(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}
