package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
)

// The CloudWatch alarms count these log lines, matching on msg and route, so their shape matters.
func TestMetricsLogsFailedAndSlowRequests(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	e := route.NewEngine(config.NewOptions(nil))
	e.Use(Metrics(20 * time.Millisecond))
	e.GET("/ok", func(_ context.Context, c *app.RequestContext) { c.String(200, "ok") })
	e.GET("/slow", func(_ context.Context, c *app.RequestContext) { time.Sleep(30 * time.Millisecond); c.String(200, "ok") })
	e.GET("/broken", func(_ context.Context, c *app.RequestContext) { c.String(503, "down") })

	for _, path := range []string{"/ok", "/slow", "/broken", "/ok"} {
		ut.PerformRequest(e, "GET", path, nil)
	}

	var got []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("not JSON: %s", line)
		}
		got = append(got, m)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 lines (slow, failed), fast successes shouldn't log: %v", got)
	}
	if got[0]["msg"] != "slow request" || got[0]["level"] != "WARN" || got[0]["route"] != "/slow" {
		t.Errorf("slow request line: %v", got[0])
	}
	if got[1]["msg"] != "request failed" || got[1]["level"] != "ERROR" || got[1]["status"] != float64(503) {
		t.Errorf("failed request line: %v", got[1])
	}
}
