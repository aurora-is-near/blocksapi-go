package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb_blocksapi "github.com/aurora-is-near/borealis-prototypes-go/generated/blocksapi"
	vtgrpc "github.com/planetscale/vtprotobuf/codec/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
)

var (
	serverAddr            = flag.String("server", "localhost:4300", "grpc endpoint address")
	streamName            = flag.String("stream", "v2_mainnet_near_blocks", "stream name")
	startOnLatest         = flag.Bool("start-on-latest", false, "start on latest block in stream")
	startExactlyOn        = flag.Uint64("start-exactly-on", 0, "start exactly on given height")
	startExactlyAfter     = flag.Uint64("start-exactly-after", 0, "start exactly after given height")
	startOn               = flag.Uint64("start-on", 0, "start on given height")
	startAfter            = flag.Uint64("start-after", 0, "start after given height")
	stopBefore            = flag.Uint64("stop-before", 0, "stop before given height")
	stopAfter             = flag.Uint64("stop-after", 0, "stop after given height")
	excludePayload        = flag.Bool("exclude-payload", false, "exclude blocks payload from responses (headers-only)")
	waitCatchup           = flag.Bool("wait-catchup", false, "allow catchup and wait for it")
	streamCatchup         = flag.Bool("stream-catchup", false, "allow catchup and stream it")
	excludeCatchupPayload = flag.Bool("exclude-catchup-payload", false, "exclude blocks payload from responses (headers-only)")
	logSpeedInterval      = flag.Uint("log-speed", 0, "log speed interval in seconds (default 0 means don't log)")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("finished with error: %v", err)
	}
}

func mkBlockID(height uint64) *pb_blocksapi.BlockMessage_ID {
	return &pb_blocksapi.BlockMessage_ID{
		Kind:   pb_blocksapi.BlockMessage_MSG_WHOLE,
		Height: height,
	}
}

func createRequest() *pb_blocksapi.ReceiveBlocksRequest {
	request := &pb_blocksapi.ReceiveBlocksRequest{
		StreamName:  *streamName,
		StartPolicy: pb_blocksapi.ReceiveBlocksRequest_START_ON_EARLIEST_AVAILABLE,
		StopPolicy:  pb_blocksapi.ReceiveBlocksRequest_STOP_NEVER,
		DeliverySettings: &pb_blocksapi.BlockStreamDeliverySettings{
			Content: &pb_blocksapi.BlockMessageDeliverySettings{
				ExcludePayload: *excludePayload,
			},
		},
		CatchupPolicy: pb_blocksapi.ReceiveBlocksRequest_CATCHUP_PANIC,
		CatchupDeliverySettings: &pb_blocksapi.BlockStreamDeliverySettings{
			Content: &pb_blocksapi.BlockMessageDeliverySettings{
				ExcludePayload: *excludeCatchupPayload,
			},
		},
	}
	if *startOnLatest {
		request.StartPolicy = pb_blocksapi.ReceiveBlocksRequest_START_ON_LATEST_AVAILABLE
	}
	if *startExactlyOn != 0 {
		request.StartPolicy = pb_blocksapi.ReceiveBlocksRequest_START_EXACTLY_ON_TARGET
		request.StartTarget = mkBlockID(*startExactlyOn)
	}
	if *startExactlyAfter != 0 {
		request.StartPolicy = pb_blocksapi.ReceiveBlocksRequest_START_EXACTLY_AFTER_TARGET
		request.StartTarget = mkBlockID(*startExactlyAfter)
	}
	if *startOn != 0 {
		request.StartPolicy = pb_blocksapi.ReceiveBlocksRequest_START_ON_CLOSEST_TO_TARGET
		request.StartTarget = mkBlockID(*startOn)
	}
	if *startAfter != 0 {
		request.StartPolicy = pb_blocksapi.ReceiveBlocksRequest_START_ON_EARLIEST_AFTER_TARGET
		request.StartTarget = mkBlockID(*startAfter)
	}
	if *stopBefore != 0 {
		request.StopPolicy = pb_blocksapi.ReceiveBlocksRequest_STOP_BEFORE_TARGET
		request.StopTarget = mkBlockID(*stopBefore)
	}
	if *stopAfter != 0 {
		request.StopPolicy = pb_blocksapi.ReceiveBlocksRequest_STOP_AFTER_TARGET
		request.StopTarget = mkBlockID(*stopAfter)
	}
	if *waitCatchup {
		request.CatchupPolicy = pb_blocksapi.ReceiveBlocksRequest_CATCHUP_WAIT
	}
	if *streamCatchup {
		request.CatchupPolicy = pb_blocksapi.ReceiveBlocksRequest_CATCHUP_STREAM
	}
	return request
}

func run() error {
	encoding.RegisterCodec(vtgrpc.Codec{})

	client, err := grpc.NewClient(
		*serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialConnWindowSize(64*1024*1024), // 64 MB
		grpc.WithInitialWindowSize(64*1024*1024),     // 64 MB
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1*1024*1024*1024), // 1 GB
		),
	)
	if err != nil {
		return fmt.Errorf("unable to create GRPC connection to %s: %v", *serverAddr, err)
	}
	defer func() {
		log.Printf("Closing connection...")
		if err := client.Close(); err != nil {
			log.Printf("Unable to close connection: %v", err)
		}
	}()

	blocksProviderClient := pb_blocksapi.NewBlocksProviderClient(client)

	// Attaching authorization token
	md := metadata.New(make(map[string]string))
	if token := os.Getenv("BLOCKSAPI_TOKEN"); len(token) > 0 {
		md.Set("authorization", "Bearer "+token)
	}
	callCtx := metadata.NewOutgoingContext(context.Background(), md)

	log.Printf("Calling...")
	callClient, err := blocksProviderClient.ReceiveBlocks(callCtx, createRequest())
	if err != nil {
		return fmt.Errorf("unable to call ReceiveBlocks: %w", err)
	}
	defer func() {
		log.Printf("Closing call...")
		if err := callClient.CloseSend(); err != nil {
			log.Printf("Unable to close call: %v", err)
		}
	}()
	log.Printf("A call was just made, waiting for responses...")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)
	defer cancel()

	headersHandled := false
	handleHeaders := func() {
		if headersHandled {
			return
		}
		md, err := callClient.Header()
		if err != nil {
			log.Printf("Still unable to get response headers: %v", err)
			return
		}
		reqids := md.Get("x-reqid")
		if len(reqids) == 0 {
			log.Printf("Got no x-reqid header :/")
		}
		for _, reqid := range reqids {
			log.Printf("x-reqid: %s", reqid)
		}
		headersHandled = true
	}
	defer handleHeaders()

	lastSpeedLogTime := time.Now()
	msgsSinceLastSpeedLog := uint64(0)
	bytesSinceLastSpeedLog := uint64(0)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Got interrupted, stopping...")
			return nil
		default:
		}

		response, err := callClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("OK, finished")
				return nil
			}
			log.Printf("unable to receive next response: (%T) %v", err, err)
			return fmt.Errorf("unable to receive next response: %w", err)
		}

		handleHeaders()

		switch r := response.Response.(type) {
		case *pb_blocksapi.ReceiveBlocksResponse_Message:
			payloadSize := 0
			if rawPayload, ok := r.Message.Message.Payload.(*pb_blocksapi.BlockMessage_RawPayload); ok {
				payloadSize = len(rawPayload.RawPayload)
			}
			log.Printf(
				"NEW MESSAGE: catchup=%v, height=%d, payloadSize=%d",
				r.Message.CatchupInProgress, r.Message.Message.Id.Height, payloadSize,
			)

			if *logSpeedInterval > 0 {
				msgsSinceLastSpeedLog++
				bytesSinceLastSpeedLog += uint64(payloadSize)
				if timeDiff := time.Since(lastSpeedLogTime); timeDiff > time.Second*time.Duration(*logSpeedInterval) {
					msgsSpeed := float64(msgsSinceLastSpeedLog) / timeDiff.Seconds()
					mbSpeed := float64(bytesSinceLastSpeedLog) / timeDiff.Seconds() / 1024 / 1024
					log.Printf("SPEED: %0.2fmsgs/sec, %0.2fMB/sec", msgsSpeed, mbSpeed)
					lastSpeedLogTime = time.Now()
					msgsSinceLastSpeedLog = 0
					bytesSinceLastSpeedLog = 0
				}
			}

		case *pb_blocksapi.ReceiveBlocksResponse_Done_:
			log.Printf("DONE: %s", r.Done.Description)
		case *pb_blocksapi.ReceiveBlocksResponse_Error_:
			log.Printf("ERROR (%v): %s", r.Error.Kind, r.Error.Description)
		}
	}
}
