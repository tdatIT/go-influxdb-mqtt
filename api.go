package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/apache/arrow-go/v18/arrow"
)

// Parameters ($id) only work in WHERE predicates, not inside INTERVAL literals.
// => range/bin go through a whitelist before being formatted into SQL (no SQL injection).
var intervals = map[string]string{
	"10s": "10 seconds", "30s": "30 seconds",
	"1m": "1 minute", "5m": "5 minutes", "15m": "15 minutes",
	"1h": "1 hour", "6h": "6 hours", "24h": "24 hours",
}

type API struct{ db *influxdb3.Client }

func (a *API) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/machines/{id}/latest", a.latest)
	mux.HandleFunc("GET /api/machines/{id}/temperature", a.temperature)
	mux.HandleFunc("GET /api/sales/summary", a.salesSummary)
	return mux
}

// Latest telemetry row of one machine
func (a *API) latest(w http.ResponseWriter, r *http.Request) {
	sql := `SELECT time, temperature, humidity, door_open, stock
	        FROM telemetry
	        WHERE machine_id = $id AND time >= now() - INTERVAL '1 hour'
	        ORDER BY time DESC
	        LIMIT 1`
	a.run(w, r, sql, influxdb3.QueryParameters{"id": r.PathValue("id")})
}

// Avg/min/max temperature per time bucket (downsampling at query time)
// GET /api/machines/vm-003/temperature?range=15m&bin=1m
func (a *API) temperature(w http.ResponseWriter, r *http.Request) {
	rng, ok1 := intervals[queryOr(r, "range", "15m")]
	bin, ok2 := intervals[queryOr(r, "bin", "1m")]
	if !ok1 || !ok2 {
		http.Error(w, "invalid range/bin", http.StatusBadRequest)
		return
	}
	sql := fmt.Sprintf(`
		SELECT date_bin(INTERVAL '%s', time) AS bucket,
		       avg(temperature) AS avg_temp,
		       min(temperature) AS min_temp,
		       max(temperature) AS max_temp
		FROM telemetry
		WHERE machine_id = $id AND time >= now() - INTERVAL '%s'
		GROUP BY 1
		ORDER BY 1`, bin, rng)
	a.run(w, r, sql, influxdb3.QueryParameters{"id": r.PathValue("id")})
}

// Revenue per machine over a time range
// GET /api/sales/summary?range=1h
func (a *API) salesSummary(w http.ResponseWriter, r *http.Request) {
	rng, ok := intervals[queryOr(r, "range", "1h")]
	if !ok {
		http.Error(w, "invalid range", http.StatusBadRequest)
		return
	}
	sql := fmt.Sprintf(`
		SELECT machine_id, count(*) AS orders, sum(price) AS revenue
		FROM sales
		WHERE time >= now() - INTERVAL '%s'
		GROUP BY machine_id
		ORDER BY revenue DESC`, rng)
	a.run(w, r, sql, nil)
}

// run executes the SQL and returns a JSON array
func (a *API) run(w http.ResponseWriter, r *http.Request, sql string, params influxdb3.QueryParameters) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var it *influxdb3.QueryIterator
	var err error
	if params != nil {
		it, err = a.db.QueryWithParameters(ctx, sql, params)
	} else {
		it, err = a.db.Query(ctx, sql)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows := []map[string]any{}
	for it.Next() {
		row := it.Value()
		for k, v := range row {
			if ts, ok := v.(arrow.Timestamp); ok {
				row[k] = ts.ToTime(arrow.Nanosecond)
			}
		}
		rows = append(rows, row)
	}
	if err := it.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rows)
}

func queryOr(r *http.Request, key, def string) string {
	if v := r.URL.Query().Get(key); v != "" {
		return v
	}
	return def
}
