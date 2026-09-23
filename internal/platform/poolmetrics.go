package platform

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// poolCollector exports connection pool stats on every scrape. The one to watch is
// db_pool_empty_acquire_wait_seconds_total: time requests spent waiting because every
// connection was busy. If it grows under load, the pool (or the database) is the bottleneck.
type poolCollector struct {
	pool *pgxpool.Pool

	acquired, max, total               *prometheus.Desc
	acquires, emptyAcquires, emptyWait *prometheus.Desc
}

// RegisterPoolMetrics exports pool's stats to Prometheus. Call it once, from main: metrics live in a
// process-wide registry, so registering a second pool (as tests do) would panic.
func RegisterPoolMetrics(pool *pgxpool.Pool) {
	prometheus.MustRegister(&poolCollector{
		pool:          pool,
		acquired:      prometheus.NewDesc("db_pool_acquired_conns", "Connections currently checked out.", nil, nil),
		max:           prometheus.NewDesc("db_pool_max_conns", "Pool size limit.", nil, nil),
		total:         prometheus.NewDesc("db_pool_total_conns", "Open connections.", nil, nil),
		acquires:      prometheus.NewDesc("db_pool_acquires_total", "Connection checkouts.", nil, nil),
		emptyAcquires: prometheus.NewDesc("db_pool_empty_acquires_total", "Checkouts that had to wait because no connection was free.", nil, nil),
		emptyWait:     prometheus.NewDesc("db_pool_empty_acquire_wait_seconds_total", "Total time spent waiting for a free connection.", nil, nil),
	})
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.acquired, c.max, c.total, c.acquires, c.emptyAcquires, c.emptyWait} {
		ch <- d
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.max, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(s.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.acquires, prometheus.CounterValue, float64(s.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquires, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyWait, prometheus.CounterValue, s.EmptyAcquireWaitTime().Seconds())
}
