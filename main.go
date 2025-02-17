package main

import (
    "context"
    "io/ioutil"
    "log"
    "os"

    mqtt "github.com/eclipse/paho.mqtt.golang"
    "gopkg.in/yaml.v2"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/sdk/trace"
    "go.opentelemetry.io/otel/trace"
)

// Config struct to hold the configuration
type Config struct {
    MQTT struct {
        Broker string   `yaml:"broker"`
        Topics []string `yaml:"topics"`
    } `yaml:"mqtt"`
    OTLP struct {
        Endpoint string `yaml:"endpoint"`
    } `yaml:"otlp"`
}

// InitializeTracer initializes an OTLP tracer
func InitializeTracer(endpoint string) (trace.Tracer, error) {
    ctx := context.Background()
    exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint))
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
    // Read the configuration file
    configFile, err := os.Open("config.yaml")
    if err != nil {
        log.Fatalf("Failed to open config file: %v", err)
    }
    defer configFile.Close()

    // Parse the configuration file
    configData, err := ioutil.ReadAll(configFile)
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

    var config Config
    if err := yaml.Unmarshal(configData, &config); err != nil {
        log.Fatalf("Failed to parse config file: %v", err)
    }

    // Create MQTT client options
    opts := mqtt.NewClientOptions()
    opts.AddBroker(config.MQTT.Broker)
    opts.SetClientID("mqtt-otlp-client")
    opts.SetDefaultPublishHandler(messageHandler)

    // Create MQTT client and connect
    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
    }
    defer client.Disconnect(250)

    // Subscribe to topics
    for _, topic := range config.MQTT.Topics {
        if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
            log.Fatalf("Failed to subscribe to topic %s: %v", topic, token.Error())
        }
    }

    // Initialize OTLP tracer
    tracer, err := InitializeTracer(config.OTLP.Endpoint)
    if err != nil {
        log.Fatalf("Failed to initialize tracer: %v", err)
    }

    // Block main goroutine to keep the application running
    select {}
}

// messageHandler handles incoming MQTT messages
func messageHandler(client mqtt.Client, msg mqtt.Message) {
    tracer := otel.Tracer("mqtt-otlp")
    ctx := context.Background()
    _, span := tracer.Start(ctx, "mqtt-message")
    defer span.End()

    // Extract message details
    topic := msg.Topic()
    payload := string(msg.Payload())

    // Log the message
    log.Printf("Received message on topic %s: %s", topic, payload)

    // Add attributes to the span
    span.SetAttributes(
        otel.LabelKeyValue{Key: "mqtt.topic", Value: otel.StringValue(topic)},
        otel.LabelKeyValue{Key: "mqtt.payload", Value: otel.StringValue(payload)},
    )
}
