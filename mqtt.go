package main

import (
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const topicFilter = "vending/+/+/+" // vending/{site}/{machine_id}/{telemetry|sale}

func ConnectMQTT(broker string, w *BatchWriter) (mqtt.Client, error) {
	onMessage := func(_ mqtt.Client, msg mqtt.Message) {
		p, err := ToPoint(msg.Topic(), msg.Payload())
		if err != nil {
			log.Printf("skipping message %s: %v", msg.Topic(), err)
			return
		}
		w.Add(p)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("vending-ingestor").
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		// Subscribe inside OnConnect so the subscription is restored after a reconnect
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Printf("MQTT connected, subscribing to %s", topicFilter)
			if t := c.Subscribe(topicFilter, 1, onMessage); t.Wait() && t.Error() != nil {
				log.Printf("subscribe failed: %v", t.Error())
			}
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("MQTT connection lost: %v", err)
		})

	c := mqtt.NewClient(opts)
	t := c.Connect()
	t.Wait()
	return c, t.Error()
}
