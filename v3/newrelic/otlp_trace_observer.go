// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build go1.9
// +build go1.9

// This build tag is necessary because GRPC/ProtoBuf libraries only support Go version 1.9 and up.

package newrelic

import (
	"context"
	"encoding/hex"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/newrelic/go-agent/v3/internal"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

type otlpTraceObserver struct {
	initialConnSuccess chan struct{}
	// initConnOnce protects initialConnSuccess from being closed multiple times.
	initConnOnce sync.Once

	initiateShutdown chan struct{}
	// initShutdownOnce protects initiateShutdown from being closed multiple times.
	initShutdownOnce sync.Once

	messages         chan *spanEvent
	shutdownComplete chan struct{}

	metadata     metadata.MD
	metadataLock sync.Mutex

	entityGUID string

	// dialOptions are the grpc.DialOptions to be used when calling grpc.Dial.
	dialOptions []grpc.DialOption

	observerConfig
}

func newOtlpTraceObserver(entityGUID string, cfg observerConfig) (traceObserver, error) {
	to := &otlpTraceObserver{
		entityGUID:         entityGUID,
		messages:           make(chan *spanEvent, cfg.queueSize),
		initialConnSuccess: make(chan struct{}),
		initiateShutdown:   make(chan struct{}),
		shutdownComplete:   make(chan struct{}),
		metadata:           newOtlpMetadata(cfg.license),
		observerConfig:     cfg,
		dialOptions:        newDialOptions(cfg),
	}
	go func() {
		to.connectToTraceObserver()

		// Closing shutdownComplete must be done before draining the messages.
		// This prevents spans from being put onto the messages channel while
		// we are trying to empty the channel.
		close(to.shutdownComplete)
		for len(to.messages) > 0 {
			// drain the channel
			<-to.messages
		}
	}()
	return to, nil
}

// newMetadata creates a grpc metadata with proper keys and values for use when
// connecting to RecordSpan.
func newOtlpMetadata(license string) metadata.MD {
	md := metadata.New(nil)
	md.Set("api-key", license)
	return md
}

// markInitialConnSuccessful closes the otlpTraceObserver initialConnSuccess channel and
// is safe to call multiple times.
func (to *otlpTraceObserver) markInitialConnSuccessful() {
	to.initConnOnce.Do(func() {
		close(to.initialConnSuccess)
	})
}

func (to *otlpTraceObserver) dumpSupportabilityMetrics() map[string]float64 {
	return nil
}

// startShutdown closes the otlpTraceObserver initiateShutdown channel and
// is safe to call multiple times.
func (to *otlpTraceObserver) startShutdown() {
	to.initShutdownOnce.Do(func() {
		close(to.initiateShutdown)
	})
}

func (to *otlpTraceObserver) connectToTraceObserver() {
	conn, err := grpc.Dial(to.endpoint.host, to.dialOptions...)
	if nil != err {
		// this error is unrecoverable and will not be retried
		to.log.Error("trace observer unable to dial grpc endpoint", map[string]interface{}{
			"host": to.endpoint.host,
			"err":  err.Error(),
		})
		return
	}
	defer to.closeConn(conn)

	serviceClient := collectortrace.NewTraceServiceClient(conn)
	ticker := time.NewTicker(recordSpanBackoff)

	for {
		select {
		case <-ticker.C:
			to.drainQueue(serviceClient)
		case <-to.initiateShutdown:
			ticker.Stop()
			to.drainQueue(serviceClient)
			return
		}
	}
}

func (to *otlpTraceObserver) closeConn(conn *grpc.ClientConn) {
	// Related to https://github.com/grpc/grpc-go/issues/2159
	// If we call conn.Close() immediately, some messages may still be
	// buffered and will never be sent. Initial testing suggests this takes
	// around 150-200ms with a full channel.
	time.Sleep(500 * time.Millisecond)
	if err := conn.Close(); nil != err {
		to.log.Info("closing trace observer connection was not successful", map[string]interface{}{
			"err": err.Error(),
		})
	}
}

func (to *otlpTraceObserver) drainQueue(spanClient collectortrace.TraceServiceClient) {
	numSpans := len(to.messages)
	spans := make([]*trace.Span, 0, numSpans)
	for i := 0; i < numSpans; i++ {
		msg := <-to.messages
		span := transformOtlpEvent(msg)
		spans = append(spans, span)
	}
	request := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{
							Key: "entity.guid",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{
									StringValue: to.entityGUID,
								},
							},
						},
					},
					DroppedAttributesCount: 0,
				},
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{Spans: spans}},
			},
		},
	}
	to.metadataLock.Lock()
	md := to.metadata
	to.metadataLock.Unlock()
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	_, err := spanClient.Export(ctx, request)
	if err != nil {
		to.log.Error(err.Error(), nil)
	}
}

// restart enqueues a request to restart with a new run ID
func (to *otlpTraceObserver) restart(runID internal.AgentRunID, requestHeadersMap map[string]string) {
}

// shutdown initiates a shutdown of the trace observer and blocks until either
// shutdown is complete (including draining existing spans from the messages channel)
// or the given timeout is hit.
func (to *otlpTraceObserver) shutdown(timeout time.Duration) error {
	to.startShutdown()
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	// Block until the observer shutdown is complete or timeout hit
	select {
	case <-to.shutdownComplete:
		return nil
	case <-ticker.C:
		return errTimeout
	}
}

// initialConnCompleted indicates that the initial connection to the remote trace
// observer was made, but it does NOT indicate anything about the current state of the
// connection
func (to *otlpTraceObserver) initialConnCompleted() bool {
	return true
}

func transformOtlpEvent(e *spanEvent) *trace.Span {
	traceId := make([]byte, 16)
	spanId := make([]byte, 8)
	parentId := make([]byte, 8)
	hex.Decode(traceId, []byte(e.TraceID))
	hex.Decode(spanId, []byte(e.GUID))
	hex.Decode(parentId, []byte(e.ParentID))

	span := &trace.Span{
		SpanId:            spanId,
		ParentSpanId:      parentId,
		TraceId:           traceId,
		Name:              e.Name,
		StartTimeUnixNano: uint64(e.Timestamp.UnixNano()),
		EndTimeUnixNano:   uint64(e.Timestamp.Add(e.Duration).UnixNano()),
	}

	return span
}

// consumeSpan enqueues the span to be sent to the remote trace observer
func (to *otlpTraceObserver) consumeSpan(span *spanEvent) {
	if to.isAppShutdownComplete() {
		return
	}

	if to.isShutdownInitiated() {
		return
	}

	select {
	case to.messages <- span:
	default:
		if to.log.DebugEnabled() {
			to.log.Debug("could not send span to trace observer because channel is full", map[string]interface{}{
				"channel size": to.queueSize,
			})
		}
	}

	return
}

// isShutdownComplete returns a bool if the trace observer has been shutdown.
func (to *otlpTraceObserver) isShutdownComplete() bool {
	return isChanClosed(to.shutdownComplete)
}

// isShutdownInitiated returns a bool if the trace observer has started
// shutting down.
func (to *otlpTraceObserver) isShutdownInitiated() bool {
	return isChanClosed(to.initiateShutdown)
}

// isAppShutdownComplete returns a bool if the trace observer's application has
// been shutdown.
func (to *otlpTraceObserver) isAppShutdownComplete() bool {
	return isChanClosed(to.appShutdown)
}
