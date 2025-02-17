package main

import (
    "context"
    "log"
    "os"
    "strings"

    mqtt "github.com/eclipse/paho.mqtt.golang"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/sdk/trace"
    "go.opentelemetry.io/otel/trace"
)

// InitializeTracer initializes an OTLP tracer
func InitializeTracer() (trace.Tracer, error) {
    ctx := context.Background()
    exporter, err := otlptracehttp.New(ctx)
    if err != nil {
        return nil, err
    }

    tp := trace.NewTracerProvider(
        trace.WithBatcher(exporter),
    )
    otel.SetTracerProvider(tp)
    return tp.Tracer("mqtt-otlp"), nil
}

func main() {
    // MQTT broker URL and topics
    broker := "tcp://localhost:1883"
    topics := []string{"topic1", "topic2"}

    // Create MQTT client options
    opts := mqtt.NewClientOptions()
    opts.AddBroker(broker)
    opts.SetClientID("mqtt-otlp-client")
    opts.SetDefaultPublishHandler(messageHandler)

    // Create MQTT client and connect
    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
    }
    defer client.Disconnect(250)

    // Subscribe to topics
    for _, topic := range topics {
        if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
            log.Fatalf("Failed to subscribe to topic %s: %v", topic, token.Error())
        }
    }

    // Initialize OTLP tracer
    tracer, err := Initialize
