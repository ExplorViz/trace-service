//go:generate go run ./scripts/genproto/genproto.go
//go:generate go run ./scripts/reghook/reghook.go

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ExplorViz/trace-service/internal/kafka/spanproc"
	"github.com/ExplorViz/trace-service/internal/kafka/tokenproc"
	"github.com/ExplorViz/trace-service/internal/token"
)

func main() {
	fs := ff.NewFlagSet("trace-service")
	var (
		seedBroker     = fs.String('b', "broker", "localhost:9091", "network endpoint of the Kafka broker to use")
		inTopic        = fs.String('i', "topic-in", "telemetry.spans.raw", "Kafka topic to consume OpenTelemetry OTLP spans from")
		outTopic       = fs.String('o', "topic-out", "telemetry.spans.parsed", "Kafka topic to produce parsed spans into")
		tokensTopic    = fs.String('t', "topic-tokens", "tokens.events", "Kafka topic to consume landscape token events from")
		validateTokens = fs.Bool('v', "validate-tokens", "whether to verify the existence of provided landscape tokens for incoming traces")
		logLevel       = fs.StringEnum('l', "log-level", "log level: info, error, debug", "info", "error", "debug")
		logInterval    = fs.DurationLong("log-interval", 5*time.Second, "interval at which received spans should be logged (0 to disable)")
	)
	if err := ff.Parse(fs, os.Args[1:], ff.WithEnvVarPrefix("EXPLORVIZ")); err != nil {
		fmt.Println(err)
		fmt.Printf("%s\n", ffhelp.Flags(fs))
		os.Exit(0)
	}

	if *logInterval < 0 {
		slog.Error("span logging duration must be positive")
		flag.Usage()
		os.Exit(1)
	}
	switch *logLevel {
	case "info":
		slog.SetLogLoggerLevel(slog.LevelInfo)
	case "error":
		slog.SetLogLoggerLevel(slog.LevelError)
	case "debug":
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	spanCl, err := kgo.NewClient(
		kgo.SeedBrokers(*seedBroker),
		kgo.ConsumeTopics(*inTopic),
		kgo.DefaultProduceTopic(*outTopic),
		kgo.ConsumerGroup("trace-service"),
	)
	if err != nil {
		slog.Error("unable to initialize kgo client for spans", "error", err)
		os.Exit(1)
	}
	defer spanCl.Close()

	tokenCl, err := kgo.NewClient(
		kgo.SeedBrokers(*seedBroker),
		kgo.ConsumeTopics(*tokensTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // replay token events from beginning
	)
	if err != nil {
		slog.Error("unable to initialize kgo client for token events", "error", err)
		os.Exit(1)
	}
	defer tokenCl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		slog.Info("received interrupt signal; gracefully stopping ...")
		cancel()
		<-sigs
		slog.Info("received second interrupt signal; exiting immediately")
		os.Exit(1)
	}()

	var wg sync.WaitGroup

	var tv token.TokenValidator
	if *validateTokens {
		inmem := token.NewInMemTokenStore()
		wg.Go(func() { tokenproc.Run(ctx, tokenCl, inmem) })
		tv = inmem
	} else {
		tv = token.NoOpTokenValidator{}
	}
	wg.Go(func() { spanproc.Run(ctx, spanCl, tv, *logInterval) })

	fmt.Print(`
  ______            _         __      ___
 |  ____|          | |        \ \    / (_)
 | |__  __  ___ __ | | ___  _ _\ \  / / _ ____
 |  __| \ \/ / '_ \| |/ _ \| '__\ \/ / | |_  /
 | |____ >  <| |_) | | (_) | |   \  /  | |/ /
 |______/_/\_\ .__/|_|\___/|_|    \/   |_/___|
             | |
             |_|                 trace-service

`)

	<-ctx.Done()
	wg.Wait()
}
