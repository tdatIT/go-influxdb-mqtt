package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

// TelemetryMsg published to vending/{site}/{machine_id}/telemetry
type TelemetryMsg struct {
	TS          int64   `json:"ts"` // epoch millis set by the device
	Temperature float64 `json:"temperature"`
	Humidity    float64 `json:"humidity"`
	DoorOpen    bool    `json:"door_open"`
	Stock       int64   `json:"stock"`
}

// SaleMsg published to vending/{site}/{machine_id}/sale
type SaleMsg struct {
	TS      int64  `json:"ts"`
	OrderID string `json:"order_id"`
	Product string `json:"product"`
	Payment string `json:"payment"`
	Price   int64  `json:"price"`
}

// ToPoint turns one MQTT message into one Point (= one line of line protocol).
// Topic: vending/{site}/{machine_id}/{kind}
func ToPoint(topic string, payload []byte) (*influxdb3.Point, error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid topic: %s", topic)
	}
	site, machineID, kind := parts[1], parts[2], parts[3]

	switch kind {
	case "telemetry":
		var m TelemetryMsg
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return influxdb3.NewPointWithMeasurement("telemetry").
			SetTag("site", site).                         // tag: used to filter / group by
			SetTag("machine_id", machineID).              // tag
			SetDoubleField("temperature", m.Temperature). // field: measured value
			SetDoubleField("humidity", m.Humidity).
			SetBooleanField("door_open", m.DoorOpen).
			SetIntegerField("stock", m.Stock).
			SetTimestamp(tsOrNow(m.TS)), nil

	case "sale":
		var m SaleMsg
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return influxdb3.NewPointWithMeasurement("sales").
			SetTag("site", site).
			SetTag("machine_id", machineID).
			SetTag("product", m.Product). // few distinct values -> tag
			SetTag("payment", m.Payment).
			SetStringField("order_id", m.OrderID). // unique per order -> field, NOT a tag
			SetIntegerField("price", m.Price).
			SetTimestamp(tsOrNow(m.TS)), nil
	}
	return nil, fmt.Errorf("unsupported kind: %s", kind)
}

func tsOrNow(ms int64) time.Time {
	if ms <= 0 {
		return time.Now()
	}
	return time.UnixMilli(ms)
}
