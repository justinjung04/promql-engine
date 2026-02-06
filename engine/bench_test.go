// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
)

func BenchmarkChunkDecoding(b *testing.B) {
	storage := setupStorage(b, 1000, 3, 720)
	defer storage.Close()

	ctx := context.Background()
	start := time.Unix(0, 0)
	end := start.Add(6 * time.Hour)
	step := time.Second * 30

	querier, err := storage.Querier(start.UnixMilli(), end.UnixMilli())
	testutil.Ok(b, err)

	matcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total")
	testutil.Ok(b, err)

	b.Run("iterate by series", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			numIterations := 0

			ss := querier.Select(ctx, false, nil, matcher)
			series := make([]chunkenc.Iterator, 0)
			for ss.Next() {
				series = append(series, ss.At().Iterator(nil))
			}
			for i := range series {
				for ts := start.UnixMilli(); ts <= end.UnixMilli(); ts += step.Milliseconds() {
					numIterations++
					if val := series[i].Seek(ts); val == chunkenc.ValNone {
						break
					}
				}
			}
		}
	})
	b.Run("iterate by time", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			numIterations := 0
			ss := querier.Select(ctx, false, nil, matcher)
			series := make([]chunkenc.Iterator, 0)
			for ss.Next() {
				series = append(series, ss.At().Iterator(nil))
			}
			stepCount := 10
			ts := start.UnixMilli()
			for ts <= end.UnixMilli() {
				for i := range series {
					seriesTs := ts
					for currStep := 0; currStep < stepCount && seriesTs <= end.UnixMilli(); currStep++ {
						numIterations++
						if valType := series[i].Seek(seriesTs); valType == chunkenc.ValNone {
							break
						}
						seriesTs += step.Milliseconds()
					}
				}
				ts += step.Milliseconds() * int64(stepCount)
			}
		}
	})
}

func BenchmarkSingleQuery(b *testing.B) {

	memProfileRate := runtime.MemProfileRate
	runtime.MemProfileRate = 0

	test := setupStorage(b, 5000, 3, 720)
	defer test.Close()

	start := time.Unix(0, 0)
	end := start.Add(6 * time.Hour)
	step := time.Second * 30

	query := "sum(rate(http_requests_total[2m]))"
	opts := engine.Opts{
		EngineOpts:        promql.EngineOpts{Timeout: 100 * time.Second},
		SelectorBatchSize: 256,
	}
	b.ReportAllocs()

	runtime.MemProfileRate = memProfileRate
	for b.Loop() {
		result := executeRangeQuery(b, query, test, start, end, step, opts)
		testutil.Ok(b, result.Err)
	}
}

func BenchmarkRangeQuery(b *testing.B) {
	samplesPerHour := 60 * 2
	sixHourDataset := setupStorage(b, 1000, 3, 6*samplesPerHour)
	defer sixHourDataset.Close()

	/*
		largeSixHourDataset := setupStorage(b, 10000, 10, 6*samplesPerHour)
		defer largeSixHourDataset.Close()

		sevenDaysAndTwoHoursDataset := setupStorage(b, 1000, 3, (7*24+2)*samplesPerHour)
		defer sevenDaysAndTwoHoursDataset.Close()
	*/

	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)
	step := time.Second * 30

	cases := []struct {
		name    string
		query   string
		step    time.Duration
		storage *teststorage.TestStorage
	}{
		{
			name:    "vector selector",
			query:   `http_requests_total`,
			storage: sixHourDataset,
		},
		{
			name:    "sum",
			query:   `sum(http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "sum by pod",
			query:   `sum by (pod) (http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "topk",
			query:   `topk(2, http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "bottomk",
			query:   `bottomk(2, http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "limitk",
			query:   `limitk(2, http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "limit_ratio",
			query:   `limit_ratio(0.2, http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "rate",
			query:   `rate(http_requests_total[1m])`,
			storage: sixHourDataset,
		},
		{
			name:    "rate with longer window",
			query:   `rate(http_requests_total[10m])`,
			storage: sixHourDataset,
			step:    5 * time.Minute,
		},
		{
			name:    "subquery",
			query:   `sum_over_time(rate(http_requests_total[1m])[10m:1m])`,
			storage: sixHourDataset,
		},
		/*
			{
				name:    "rate with large range selection",
				query:   "rate(http_requests_total[7d])",
				storage: sevenDaysAndTwoHoursDataset,
			},
			{
				name:    "rate with large number of series, 1m range",
				query:   "rate(http_requests_total[1m])",
				storage: largeSixHourDataset,
			},
			{
				name:    "rate with large number of series, 5m range",
				query:   "rate(http_requests_total[5m])",
				storage: largeSixHourDataset,
			},
		*/
		{
			name:    "sum rate",
			query:   `sum(rate(http_requests_total[1m]))`,
			storage: sixHourDataset,
		},
		{
			name:    "sum by rate",
			query:   `sum by (pod) (rate(http_requests_total[1m]))`,
			storage: sixHourDataset,
		},
		{
			name:    "quantile with variable parameter",
			query:   `quantile by (pod) (scalar(min(http_requests_total)), http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "binary operation with one to one",
			query:   `http_requests_total{container="c1"} / ignoring (container) http_responses_total`,
			storage: sixHourDataset,
		},
		{
			name:    "binary operation with many to one",
			query:   `http_requests_total / on (pod) group_left () http_responses_total`,
			storage: sixHourDataset,
		},
		{
			name:    "binary operation with vector and scalar",
			query:   `http_requests_total * 10`,
			storage: sixHourDataset,
		},
		{
			name:    "unary negation",
			query:   `-http_requests_total`,
			storage: sixHourDataset,
		},
		{
			name:    "vector and scalar comparison",
			query:   `http_requests_total > 10`,
			storage: sixHourDataset,
		},
		{
			name:    "positive offset vector",
			query:   `http_requests_total offset 5m`,
			storage: sixHourDataset,
		},
		{
			name:    "at modifier ",
			query:   `http_requests_total @ 600.000`,
			storage: sixHourDataset,
		},
		{
			name:    "at modifier with positive offset vector",
			query:   `http_requests_total @ 600.000 offset 5m`,
			storage: sixHourDataset,
		},
		{
			name:    "clamp",
			query:   `clamp(http_requests_total, 5, 10)`,
			storage: sixHourDataset,
		},
		{
			name:    "clamp_min",
			query:   `clamp_min(http_requests_total, 10)`,
			storage: sixHourDataset,
		},
		{
			name:    "complex func query",
			query:   `clamp(1 - http_requests_total, 10 - 5, 10)`,
			storage: sixHourDataset,
		},
		{
			name:    "func within func query",
			query:   `clamp(irate(http_requests_total[30s]), 10 - 5, 10)`,
			storage: sixHourDataset,
		},
		{
			name:    "aggr within func query",
			query:   `clamp(rate(http_requests_total[30s]), 10 - 5, 10)`,
			storage: sixHourDataset,
		},
		{
			name:    "histogram_quantile",
			query:   `histogram_quantile(0.9, http_response_seconds_bucket)`,
			storage: sixHourDataset,
		},
		{
			name:    "sort",
			query:   `sort(http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "sort_desc",
			query:   `sort_desc(http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "absent and exists",
			query:   `absent(http_requests_total)`,
			storage: sixHourDataset,
		},
		{
			name:    "absent and doesnt exist",
			query:   `absent(nonexistent)`,
			storage: sixHourDataset,
		},
		{
			name:    "double exponential smoothing",
			query:   `double_exponential_smoothing(http_requests_total[1m], 0.1, 0.1)`,
			storage: sixHourDataset,
		},
		// over_time functions
		{
			name:    "count_over_time_5m",
			query:   `count_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "count_over_time_1h",
			query:   `count_over_time(http_requests_total[1h])`,
			storage: sixHourDataset,
		},
		{
			name:    "count_over_time_6h",
			query:   `count_over_time(http_requests_total[6h])`,
			storage: sixHourDataset,
		},
		{
			name:    "sum_over_time_5m",
			query:   `sum_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "sum_over_time_1h",
			query:   `sum_over_time(http_requests_total[1h])`,
			storage: sixHourDataset,
		},
		{
			name:    "sum_over_time_6h",
			query:   `sum_over_time(http_requests_total[6h])`,
			storage: sixHourDataset,
		},

		{
			name:    "avg_over_time_5m",
			query:   `avg_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "avg_over_time_1h",
			query:   `avg_over_time(http_requests_total[1h])`,
			storage: sixHourDataset,
		},
		{
			name:    "avg_over_time_6h",
			query:   `avg_over_time(http_requests_total[6h])`,
			storage: sixHourDataset,
		},
		{
			name:    "min_over_time_5m",
			query:   `min_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "min_over_time_1h",
			query:   `min_over_time(http_requests_total[1h])`,
			storage: sixHourDataset,
		},
		{
			name:    "min_over_time_6h",
			query:   `min_over_time(http_requests_total[6h])`,
			storage: sixHourDataset,
		},
		{
			name:    "max_over_time_5m",
			query:   `max_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "max_over_time_1h",
			query:   `max_over_time(http_requests_total[1h])`,
			storage: sixHourDataset,
		},
		{
			name:    "max_over_time_6h",
			query:   `max_over_time(http_requests_total[6h])`,
			storage: sixHourDataset,
		},
		{
			name:    "stddev_over_time_5m",
			query:   `stddev_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "stddev_over_time_1h",
			query:   `stddev_over_time(http_requests_total[1h])`,
			storage: sixHourDataset,
		},
		{
			name:    "stddev_over_time_6h",
			query:   `stddev_over_time(http_requests_total[6h])`,
			storage: sixHourDataset,
		},
		{
			name:    "stdvar_over_time",
			query:   `stdvar_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "last_over_time",
			query:   `last_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
		{
			name:    "present_over_time",
			query:   `present_over_time(http_requests_total[5m])`,
			storage: sixHourDataset,
		},
	}

	opts := engine.Opts{
		EngineOpts: promql.EngineOpts{
			Logger:               nil,
			Reg:                  nil,
			MaxSamples:           50000000,
			Timeout:              100 * time.Second,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
		},
		SelectorBatchSize: 256,
	}

	for _, tc := range cases {
		testStep := step
		if tc.step != 0 {
			testStep = tc.step
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.Run("old_engine", func(b *testing.B) {

				promEngine := promql.NewEngine(opts.EngineOpts)

				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					qry, err := promEngine.NewRangeQuery(context.Background(), tc.storage, nil, tc.query, start, end, testStep)
					testutil.Ok(b, err)

					oldResult := qry.Exec(context.Background())
					testutil.Ok(b, oldResult.Err)
				}
			})
			b.Run("new_engine", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()

				for b.Loop() {
					newResult := executeRangeQuery(b, tc.query, tc.storage, start, end, testStep, opts)
					testutil.Ok(b, newResult.Err)
				}
			})
		})
	}
}

func BenchmarkNativeHistograms(b *testing.B) {
	storage := teststorage.New(b)
	defer storage.Close()

	app := storage.Appender(context.TODO())
	testutil.Ok(b, generateNativeHistogramSeries(app, 3000, false))
	testutil.Ok(b, app.Commit())

	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)
	step := time.Second * 30

	cases := []struct {
		name  string
		query string
		step  time.Duration
	}{
		{
			name:  "selector",
			query: `native_histogram_series`,
		},
		{
			name:  "sum",
			query: `sum(native_histogram_series)`,
		},
		{
			name:  "rate",
			query: `rate(native_histogram_series[1m])`,
		},
		{
			name:  "rate with longer window",
			query: `rate(native_histogram_series[10m])`,
			step:  5 * time.Minute,
		},
		{
			name:  "sum rate",
			query: `sum(rate(native_histogram_series[1m]))`,
		},
		{
			name:  "histogram_sum",
			query: `histogram_sum(native_histogram_series)`,
		},
		{
			name:  "histogram_count with rate",
			query: `histogram_count(rate(native_histogram_series[1m]))`,
		},
		{
			name:  "histogram_count",
			query: `histogram_count(native_histogram_series)`,
		},
		{
			name:  "histogram_count with sum and rate",
			query: `histogram_count(sum(rate(native_histogram_series[1m])))`,
		},
		{
			name:  "histogram_avg",
			query: `histogram_avg(native_histogram_series)`,
		},
		{
			name:  "histogram_avg with sum and rate",
			query: `histogram_avg(sum(rate(native_histogram_series[1m])))`,
		},
		{
			name:  "histogram_quantile",
			query: `histogram_quantile(0.9, sum(native_histogram_series))`,
		},
		{
			name:  "histogram scalar binop",
			query: `sum(native_histogram_series * 60)`,
		},
		{
			name:  "histogram_stdvar",
			query: `histogram_stdvar(native_histogram_series)`,
		},
		{
			name:  "histogram_stddev",
			query: `histogram_stddev(native_histogram_series)`,
		},
	}

	opts := promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           50000000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			testStep := step
			if tc.step != 0 {
				testStep = tc.step
			}
			b.Run("old_engine", func(b *testing.B) {
				engine := promql.NewEngine(opts)

				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					qry, err := engine.NewRangeQuery(context.Background(), storage, nil, tc.query, start, end, testStep)
					testutil.Ok(b, err)

					oldResult := qry.Exec(context.Background())
					testutil.Ok(b, oldResult.Err)
				}
			})
			b.Run("new_engine", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()

				for b.Loop() {
					ng := engine.New(engine.Opts{
						EngineOpts: opts,
					})

					qry, err := ng.NewRangeQuery(context.Background(), storage, nil, tc.query, start, end, testStep)
					testutil.Ok(b, err)

					newResult := qry.Exec(context.Background())
					testutil.Ok(b, newResult.Err)
				}
			})
		})
	}
}

func BenchmarkInstantQuery(b *testing.B) {
	storage := setupStorage(b, 1000, 3, 720)
	defer storage.Close()

	// 6 hour dataset at 30s intervals for long range queries
	sixHourStorage := setupStorage(b, 1000, 3, 6*60*2)
	defer sixHourStorage.Close()

	queryTime := time.Unix(50, 0)
	sixHourQueryTime := time.Unix(6*60*60, 0) // End of 6h dataset

	cases := []struct {
		name  string
		query string
	}{
		{
			name:  "vector selector",
			query: `http_requests_total`,
		},
		{
			name:  "count",
			query: `count(http_requests_total)`,
		},
		{
			name:  "count_values",
			query: `count_values("val", http_requests_total)`,
		},
		{
			name:  "round",
			query: `round(http_requests_total)`,
		},
		{
			name:  "round with argument",
			query: `round(http_requests_total, 0.5)`,
		},
		{
			name:  "avg",
			query: `avg(http_requests_total)`,
		},
		{
			name:  "sum",
			query: `sum(http_requests_total)`,
		},
		{
			name:  "sum by pod",
			query: `sum by (pod) (http_requests_total)`,
		},
		{
			name:  "rate",
			query: `rate(http_requests_total[1m])`,
		},
		{
			name:  "rate with long window",
			query: `rate(http_requests_total[1h])`,
		},
		{
			name:  "sum rate",
			query: `sum(rate(http_requests_total[1m]))`,
		},
		{
			name:  "sum by rate",
			query: `sum by (pod) (rate(http_requests_total[1m]))`,
		},
		{
			name:  "binary operation with many to one",
			query: `http_requests_total / on (pod) group_left () http_responses_total`,
		},
		{
			name:  "unary negation",
			query: `-http_requests_total`,
		},
		{
			name:  "vector and scalar comparison",
			query: `http_requests_total > 10`,
		},
		{
			name:  "sort",
			query: `sort(http_requests_total)`,
		},
		{
			name:  "sort_desc",
			query: `sort_desc(http_requests_total)`,
		},
		{
			name:  "subquery sum_over_time",
			query: `sum_over_time(count(http_requests_total)[1h:10s])`,
		},
		{
			name:  "double exponential smoothing",
			query: `double_exponential_smoothing(http_requests_total[1m], 0.1, 0.1)`,
		},
	}

	// Long range instant query cases - these benefit from OverTimeBuffer
	longRangeCases := []struct {
		name  string
		query string
	}{
		{
			name:  "count_over_time 6h",
			query: `count_over_time(http_requests_total[6h])`,
		},
		{
			name:  "sum_over_time 6h",
			query: `sum_over_time(http_requests_total[6h])`,
		},
		{
			name:  "avg_over_time 6h",
			query: `avg_over_time(http_requests_total[6h])`,
		},
		{
			name:  "min_over_time 6h",
			query: `min_over_time(http_requests_total[6h])`,
		},
		{
			name:  "max_over_time 6h",
			query: `max_over_time(http_requests_total[6h])`,
		},
		{
			name:  "stddev_over_time 6h",
			query: `stddev_over_time(http_requests_total[6h])`,
		},
		{
			name:  "stdvar_over_time 6h",
			query: `stdvar_over_time(http_requests_total[6h])`,
		},
		{
			name:  "present_over_time 6h",
			query: `present_over_time(http_requests_total[6h])`,
		},
		{
			name:  "last_over_time 6h",
			query: `last_over_time(http_requests_total[6h])`,
		},
	}

	for _, tc := range longRangeCases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("new_engine", func(b *testing.B) {
				ng := engine.New(engine.Opts{
					EngineOpts: promql.EngineOpts{Timeout: 100 * time.Second},
				})
				b.ResetTimer()
				b.ReportAllocs()

				for b.Loop() {
					qry, err := ng.NewInstantQuery(context.Background(), sixHourStorage, nil, tc.query, sixHourQueryTime)
					testutil.Ok(b, err)

					res := qry.Exec(context.Background())
					testutil.Ok(b, res.Err)
				}
			})
		})
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("old_engine", func(b *testing.B) {
				opts := promql.EngineOpts{
					Logger:               nil,
					Reg:                  nil,
					MaxSamples:           50000000,
					Timeout:              100 * time.Second,
					EnableAtModifier:     true,
					EnableNegativeOffset: true,
				}
				engine := promql.NewEngine(opts)

				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					qry, err := engine.NewInstantQuery(context.Background(), storage, nil, tc.query, queryTime)
					testutil.Ok(b, err)

					res := qry.Exec(context.Background())
					testutil.Ok(b, res.Err)
				}
			})
			b.Run("new_engine", func(b *testing.B) {
				ng := engine.New(engine.Opts{
					EngineOpts: promql.EngineOpts{Timeout: 100 * time.Second},
				})
				b.ResetTimer()
				b.ReportAllocs()

				for b.Loop() {
					qry, err := ng.NewInstantQuery(context.Background(), storage, nil, tc.query, queryTime)
					testutil.Ok(b, err)

					res := qry.Exec(context.Background())
					testutil.Ok(b, res.Err)
				}
			})
		})
	}
}

func BenchmarkMergeSelectorsOptimizer(b *testing.B) {
	db := createRequestsMetricBlock(b, 10000, 9900)
	defer db.Close()

	start := time.Unix(0, 0)
	end := start.Add(6 * time.Hour)
	step := time.Second * 30

	query := `sum(http_requests_total{code="200"}) / sum(http_requests_total)`
	b.Run("withoutOptimizers", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			opts := engine.Opts{
				LogicalOptimizers: logicalplan.NoOptimizers,
				EngineOpts:        promql.EngineOpts{Timeout: 100 * time.Second},
			}
			ng := engine.New(opts)
			ctx := context.Background()
			qry, err := ng.NewRangeQuery(ctx, db, nil, query, start, end, step)
			testutil.Ok(b, err)

			res := qry.Exec(ctx)
			testutil.Ok(b, res.Err)
		}
	})
	b.Run("withOptimizers", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			ng := engine.New(engine.Opts{EngineOpts: promql.EngineOpts{Timeout: 100 * time.Second}})
			ctx := context.Background()
			qry, err := ng.NewRangeQuery(ctx, db, nil, query, start, end, step)
			testutil.Ok(b, err)

			res := qry.Exec(ctx)
			testutil.Ok(b, res.Err)
		}
	})

}

func executeRangeQuery(b *testing.B, q string, storage *teststorage.TestStorage, start time.Time, end time.Time, step time.Duration, opts engine.Opts) *promql.Result {
	return executeRangeQueryWithOpts(b, q, storage, start, end, step, opts)
}

func executeRangeQueryWithOpts(b *testing.B, q string, storage *teststorage.TestStorage, start time.Time, end time.Time, step time.Duration, opts engine.Opts) *promql.Result {
	ng := engine.New(opts)
	ctx := context.Background()
	qry, err := ng.NewRangeQuery(ctx, storage, nil, q, start, end, step)
	testutil.Ok(b, err)

	return qry.Exec(ctx)
}

// nolint: unparam
func setupStorage(b *testing.B, numLabelsA int, numLabelsB int, numSteps int) *teststorage.TestStorage {
	load := synthesizeLoad(numLabelsA, numLabelsB, numSteps)
	return promqltest.LoadedStorage(b, load)
}

func createRequestsMetricBlock(b *testing.B, numRequests int, numSuccess int) *tsdb.DB {
	dir := b.TempDir()

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	testutil.Ok(b, err)
	appender := db.Appender(context.Background())

	sixHours := int64(6 * 60 * 2)

	for i := range numRequests {
		for t := int64(0); t < sixHours; t += 30 {
			code := "200"
			if numSuccess < i {
				code = "500"
			}
			lbls := labels.FromStrings(labels.MetricName, "http_requests_total", "code", code, "pod", strconv.Itoa(i))
			_, err = appender.Append(0, lbls, t, 1)
			testutil.Ok(b, err)
		}
	}

	testutil.Ok(b, appender.Commit())

	return db
}

func synthesizeLoad(numPods, numContainers, numSteps int) string {
	var sb strings.Builder
	sb.WriteString("load 30s\n")
	for i := range numPods {
		for j := range numContainers {
			sb.WriteString(fmt.Sprintf(`http_requests_total{pod="p%d", container="c%d"} %d+%dx%d%s`, i, j, i, j, numSteps, "\n"))
		}
		sb.WriteString(fmt.Sprintf(`http_responses_total{pod="p%d"} %dx%d%s`, i, i, numSteps, "\n"))
	}

	for i := range numPods {
		for j := range 10 {
			sb.WriteString(fmt.Sprintf(`http_response_seconds_bucket{pod="p%d", le="%d"} %d+%dx%d%s`, i, j, i, j, numSteps, "\n"))
		}
		sb.WriteString(fmt.Sprintf(`http_response_seconds_bucket{pod="p%d", le="+Inf"} %d+%dx%d%s`, i, i, i, numSteps, "\n"))
	}

	return sb.String()
}

// BenchmarkCardinalityMemory benchmarks memory consumption for queries with
// the same total number of samples but different series cardinality.
// This helps understand how memory scales with cardinality vs sample count.
func BenchmarkCardinalityMemory(b *testing.B) {
	// Target: 120,000 total samples across all series (constant)
	// Cardinality: 10, 100, 1000, 10000 series (each × 3 containers)
	// We adjust the range selector window to keep total samples constant

	cases := []struct {
		name        string
		numPods     int
		numSteps    int
		query       string
		queryTime   time.Time
		description string
	}{
		// Rate function tests
		{
			name:        "rate_cardinality_3_series",
			numPods:     1,                                // 1 pod × 3 containers = 3 series
			numSteps:    2880,                             // 24h of data at 30s intervals
			query:       `rate(http_requests_total[20h])`, // ~2400 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "3 series × 2400 samples = 7,200 total samples",
		},
		{
			name:        "rate_cardinality_30_series",
			numPods:     10,                              // 10 pods × 3 containers = 30 series
			numSteps:    2880,                            // 24h of data at 30s intervals
			query:       `rate(http_requests_total[2h])`, // ~240 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "30 series × 240 samples = 7,200 total samples",
		},
		{
			name:        "rate_cardinality_300_series",
			numPods:     100,                              // 100 pods × 3 containers = 300 series
			numSteps:    2880,                             // 24h of data at 30s intervals
			query:       `rate(http_requests_total[12m])`, // ~24 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "300 series × 24 samples = 7,200 total samples",
		},
		{
			name:        "rate_cardinality_3000_series",
			numPods:     1000,                               // 1000 pods × 3 containers = 3,000 series
			numSteps:    2880,                               // 24h of data at 30s intervals
			query:       `rate(http_requests_total[1m12s])`, // ~2.4 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "3,000 series × 2.4 samples = 7,200 total samples",
		},
		// Sum over time tests (http_responses_total: numPods series)
		{
			name:        "sum_over_time_cardinality_3_series",
			numPods:     1,                                          // 1 pod × 3 containers = 3 series
			numSteps:    2880,                                       // 24h of data at 30s intervals
			query:       `sum_over_time(http_responses_total[20h])`, // ~2400 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "3 series × 2400 samples = 7,200 total samples",
		},
		{
			name:        "sum_over_time_cardinality_30_series",
			numPods:     10,                                        // 10 pods × 3 containers = 30 series
			numSteps:    2880,                                      // 24h of data at 30s intervals
			query:       `sum_over_time(http_responses_total[2h])`, // ~240 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "30 series × 240 samples = 7,200 total samples",
		},
		{
			name:        "sum_over_time_cardinality_300_series",
			numPods:     100,                                        // 100 pods × 3 containers = 300 series
			numSteps:    2880,                                       // 24h of data at 30s intervals
			query:       `sum_over_time(http_responses_total[12m])`, // ~24 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "300 series × 24 samples = 7,200 total samples",
		},
		{
			name:        "sum_over_time_cardinality_3000_series",
			numPods:     1000,                                         // 1000 pods × 3 containers = 3000 series
			numSteps:    2880,                                         // 24h of data at 30s intervals
			query:       `sum_over_time(http_responses_total[1m12s])`, // ~2.4 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "3,000 series × 2.4 samples = 7,200 total samples",
		},
		// Histogram quantile tests (http_response_seconds_bucket: numPods × 11 buckets)
		{
			name:        "histogram_quantile_cardinality_11_series",
			numPods:     1,                                                                   // 1 pod × 11 buckets = 11 series
			numSteps:    2880,                                                                // 24h of data at 30s intervals
			query:       `histogram_quantile(0.95, rate(http_response_seconds_bucket[11h]))`, // ~660 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "11 series × 660 samples = 7,260 total samples",
		},
		{
			name:        "histogram_quantile_cardinality_110_series",
			numPods:     10,                                                                   // 10 pods × 11 buckets = 110 series
			numSteps:    2880,                                                                 // 24h of data at 30s intervals
			query:       `histogram_quantile(0.95, rate(http_response_seconds_bucket[1h6m]))`, // ~66 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "110 series × 66 samples = 7,260 total samples",
		},
		{
			name:        "histogram_quantile_cardinality_1100_series",
			numPods:     100,                                                                   // 100 pods × 11 buckets = 1,100 series
			numSteps:    2880,                                                                  // 24h of data at 30s intervals
			query:       `histogram_quantile(0.95, rate(http_response_seconds_bucket[3m18s]))`, // ~6.6 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "1,100 series × 6.6 samples = 7,260 total samples",
		},
		{
			name:        "histogram_quantile_cardinality_11000_series",
			numPods:     1000,                                                                // 1000 pods × 11 buckets = 11,000 series
			numSteps:    2880,                                                                // 24h of data at 30s intervals
			query:       `histogram_quantile(0.95, rate(http_response_seconds_bucket[20s]))`, // ~0.66 samples per series
			queryTime:   time.Unix(24*60*60, 0),
			description: "11,000 series × 0.66 samples = 7,260 total samples",
		},
	}

	opts := engine.Opts{
		EngineOpts: promql.EngineOpts{
			Logger:               nil,
			Reg:                  nil,
			MaxSamples:           50000000,
			Timeout:              100 * time.Second,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			// Setup storage with specific cardinality
			// numPods × 3 containers = total series count
			storage := setupStorage(b, tc.numPods, 3, tc.numSteps)
			defer storage.Close()

			ng := engine.New(opts)

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				qry, err := ng.NewInstantQuery(context.Background(), storage, nil, tc.query, tc.queryTime)
				testutil.Ok(b, err)

				res := qry.Exec(context.Background())
				testutil.Ok(b, res.Err)
			}
		})
	}
}

// BenchmarkSamplesScannedMemory measures how memory consumption scales with the amount
// of raw data scanned from storage (the range selector window size). Tests three query
// types (rate, sum_over_time, histogram_quantile) with varying time windows (1m to 24h)
// to understand the relationship between samples scanned and memory usage.
func BenchmarkSamplesScannedMemory(b *testing.B) {
	// Setup: 24-hour dataset with 30s intervals
	// - 1000 pods × 3 containers = 3,000 series (http_requests_total)
	// - 1000 pods × 11 buckets = 11,000 series (http_response_seconds_bucket)
	// - 2,880 samples per series (24h * 60min * 2 samples/min)
	storage := setupStorage(b, 1000, 3, 2880)
	defer storage.Close()

	// Query at end of dataset to have full 24h of data available
	queryTime := time.Unix(24*60*60, 0)

	cases := []struct {
		name             string
		query            string
		description      string
		samplesScanned   int
		samplesProcessed int
		outputSamples    int
	}{
		{
			name:             "subquery_1m_window",
			query:            `rate(http_requests_total[1m:1m])`,
			description:      "Subquery with 1m window, scans 1m of data, processes 1 sample per series",
			samplesScanned:   2,
			samplesProcessed: 1,
			outputSamples:    1,
		},
		{
			name:             "subquery_1h_window",
			query:            `rate(http_requests_total[1h:1h])`,
			description:      "Subquery with 1h window, scans 1h of data, processes 1 sample per series",
			samplesScanned:   120,
			samplesProcessed: 1,
			outputSamples:    1,
		},
		{
			name:             "subquery_6h_window",
			query:            `rate(http_requests_total[6h:6h])`,
			description:      "Subquery with 6h window, scans 6h of data, processes 1 sample per series",
			samplesScanned:   720,
			samplesProcessed: 1,
			outputSamples:    1,
		},
		{
			name:             "subquery_24h_window",
			query:            `rate(http_requests_total[24h:24h])`,
			description:      "Subquery with 24h window, scans 24h of data, processes 1 sample per series",
			samplesScanned:   2880,
			samplesProcessed: 1,
			outputSamples:    1,
		},
		{
			name:             "sum_over_time_1m_window",
			query:            `sum_over_time(http_requests_total[1m])`,
			description:      "sum_over_time with 1m window, scans 1m of data",
			samplesScanned:   2,
			samplesProcessed: 2,
			outputSamples:    1,
		},
		{
			name:             "sum_over_time_1h_window",
			query:            `sum_over_time(http_requests_total[1h])`,
			description:      "sum_over_time with 1h window, scans 1h of data",
			samplesScanned:   120,
			samplesProcessed: 120,
			outputSamples:    1,
		},
		{
			name:             "sum_over_time_6h_window",
			query:            `sum_over_time(http_requests_total[6h])`,
			description:      "sum_over_time with 6h window, scans 6h of data",
			samplesScanned:   720,
			samplesProcessed: 720,
			outputSamples:    1,
		},
		{
			name:             "sum_over_time_24h_window",
			query:            `sum_over_time(http_requests_total[24h])`,
			description:      "sum_over_time with 24h window, scans 24h of data",
			samplesScanned:   2880,
			samplesProcessed: 2880,
			outputSamples:    1,
		},
		{
			name:             "histogram_quantile_1m_window",
			query:            `histogram_quantile(0.5, rate(http_response_seconds_bucket[1m]))`,
			description:      "histogram_quantile with 1m window, scans 1m of data",
			samplesScanned:   2,
			samplesProcessed: 1,
			outputSamples:    1,
		},
		{
			name:             "histogram_quantile_1h_window",
			query:            `histogram_quantile(0.5, rate(http_response_seconds_bucket[1h]))`,
			description:      "histogram_quantile with 1h window, scans 1h of data",
			samplesScanned:   120,
			samplesProcessed: 1,
			outputSamples:    1,
		},
		{
			name:             "histogram_quantile_6h_window",
			query:            `histogram_quantile(0.5, rate(http_response_seconds_bucket[6h]))`,
			description:      "histogram_quantile with 6h window, scans 6h of data",
			samplesScanned:   720,
			samplesProcessed: 1,
			outputSamples:    1,
		},
		{
			name:             "histogram_quantile_24h_window",
			query:            `histogram_quantile(0.5, rate(http_response_seconds_bucket[24h]))`,
			description:      "histogram_quantile with 24h window, scans 24h of data",
			samplesScanned:   2880,
			samplesProcessed: 1,
			outputSamples:    1,
		},
	}

	opts := engine.Opts{
		EngineOpts: promql.EngineOpts{
			Logger:               nil,
			Reg:                  nil,
			MaxSamples:           50000000,
			Timeout:              100 * time.Second,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ng := engine.New(opts)

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				qry, err := ng.NewInstantQuery(context.Background(), storage, nil, tc.query, queryTime)
				testutil.Ok(b, err)

				res := qry.Exec(context.Background())
				testutil.Ok(b, res.Err)
			}
		})
	}
}

// BenchmarkSamplesProcessedMemory validates whether "samples processed" impacts memory
// when samples scanned are held constant. Uses subquery resolution parameter to vary
// the number of evaluations while keeping the time window constant.
func BenchmarkSamplesProcessedMemory(b *testing.B) {
	// Create a 24-hour dataset with 30s intervals (2880 samples per series)
	// numPods=1000, numContainers=3, numSteps=2880 (24h * 60min * 2 samples/min)
	storage := setupStorage(b, 1000, 3, 2880)
	defer storage.Close()

	// Query at the end of the dataset
	queryTime := time.Unix(24*60*60, 0)

	cases := []struct {
		name           string
		query          string
		description    string
		samplesScanned int
		evaluations    int
		samplesPerEval int
		totalProcessed int64
		outputSamples  int
	}{
		{
			name:           "subquery_24h_resolution_24h",
			query:          `rate(http_requests_total[24h:24h])`,
			description:    "24h window, evaluate once (at 24h), minimal processing",
			samplesScanned: 2880,
			evaluations:    1,
			samplesPerEval: 2880,
			totalProcessed: 2880,
			outputSamples:  1,
		},
		{
			name:           "subquery_24h_resolution_1h",
			query:          `rate(http_requests_total[24h:1h])`,
			description:    "24h window, evaluate every 1h (24 times), moderate processing",
			samplesScanned: 2880,
			evaluations:    24,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 24, // 69,120
			outputSamples:  24,
		},
		{
			name:           "subquery_24h_resolution_15m",
			query:          `rate(http_requests_total[24h:15m])`,
			description:    "24h window, evaluate every 15m (96 times), high processing",
			samplesScanned: 2880,
			evaluations:    96,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 96, // 276,480
			outputSamples:  96,
		},
		{
			name:           "subquery_24h_resolution_1m",
			query:          `rate(http_requests_total[24h:1m])`,
			description:    "24h window, evaluate every 1m (1440 times), massive processing",
			samplesScanned: 2880,
			evaluations:    1440,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 1440, // 4,147,200
			outputSamples:  1440,
		},
		{
			name:           "histogram_quantile_24h_resolution_24h",
			query:          `histogram_quantile(0.5, rate(http_response_seconds_bucket[24h:24h]))`,
			description:    "histogram_quantile 24h window, evaluate once (at 24h), minimal processing",
			samplesScanned: 2880,
			evaluations:    1,
			samplesPerEval: 2880,
			totalProcessed: 2880,
			outputSamples:  1,
		},
		{
			name:           "histogram_quantile_24h_resolution_1h",
			query:          `histogram_quantile(0.5, rate(http_response_seconds_bucket[24h:1h]))`,
			description:    "histogram_quantile 24h window, evaluate every 1h (24 times), moderate processing",
			samplesScanned: 2880,
			evaluations:    24,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 24, // 69,120
			outputSamples:  24,
		},
		{
			name:           "histogram_quantile_24h_resolution_15m",
			query:          `histogram_quantile(0.5, rate(http_response_seconds_bucket[24h:15m]))`,
			description:    "histogram_quantile 24h window, evaluate every 15m (96 times), high processing",
			samplesScanned: 2880,
			evaluations:    96,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 96, // 276,480
			outputSamples:  96,
		},
		{
			name:           "histogram_quantile_24h_resolution_1m",
			query:          `histogram_quantile(0.5, rate(http_response_seconds_bucket[24h:1m]))`,
			description:    "histogram_quantile 24h window, evaluate every 1m (1440 times), massive processing",
			samplesScanned: 2880,
			evaluations:    1440,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 1440, // 4,147,200
			outputSamples:  1440,
		},
	}

	opts := engine.Opts{
		EngineOpts: promql.EngineOpts{
			Logger:               nil,
			Reg:                  nil,
			MaxSamples:           50000000,
			Timeout:              100 * time.Second,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ng := engine.New(opts)

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				qry, err := ng.NewInstantQuery(context.Background(), storage, nil, tc.query, queryTime)
				testutil.Ok(b, err)

				res := qry.Exec(context.Background())
				testutil.Ok(b, res.Err)
			}
		})
	}
}

// BenchmarkSamplesProcessedOverlap validates whether overlapping windows in range queries
// have similar memory impact as subqueries with multiple evaluations.
// Compares with BenchmarkSamplesProcessedMemory to see if overlap behaves similarly.
func BenchmarkSamplesProcessedOverlapMemory(b *testing.B) {
	// Create a 24-hour dataset with 30s intervals (2880 samples per series)
	// numPods=1000, numContainers=3, numSteps=2880 (24h * 60min * 2 samples/min)
	storage := setupStorage(b, 1000, 3, 2880)
	defer storage.Close()

	// All queries: same number of evaluations (23), varying samples per evaluation
	cases := []struct {
		name           string
		query          string
		start          time.Time
		end            time.Time
		step           time.Duration
		description    string
		samplesScanned int
		evaluations    int
		samplesPerEval int
		totalProcessed int64
	}{
		{
			name:           "range_24h_window_1h_step_1h",
			query:          `rate(http_requests_total[1h])`,
			start:          time.Unix(0, 0),
			end:            time.Unix(24*60*60, 0),
			step:           1 * time.Hour,
			description:    "24h range, 1h window, 1h step (24 evaluations, minimal processing)",
			samplesScanned: 2880 + 120, // 24h + 1h lookback
			evaluations:    24,
			samplesPerEval: 120,
			totalProcessed: 120 * 24, // 2,880
		},
		{
			name:           "range_12h_window_12h_step_30m",
			query:          `rate(http_requests_total[12h])`,
			start:          time.Unix(12*60*60, 0), // 12h
			end:            time.Unix(24*60*60, 0), // 24h
			step:           30 * time.Minute,
			description:    "12h range, 12h window, 30m step (24 evaluations, high processing)",
			samplesScanned: 2880, // 24h (12h + 12h lookback)
			evaluations:    24,
			samplesPerEval: 1440,
			totalProcessed: 1440 * 24, // 34,560
		},
		{
			name:           "range_1h_window_24h_step_2m30s",
			query:          `rate(http_requests_total[24h])`,
			start:          time.Unix(23*60*60, 0), // 23h
			end:            time.Unix(24*60*60, 0), // 24h
			step:           150 * time.Second,      // 2m30s
			description:    "1h range, 24h window, 2m30s step (24 evaluations, massive processing)",
			samplesScanned: 2880, // 24h (1h + 24h lookback, but capped at dataset)
			evaluations:    24,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 24, // 69,120
		},
		{
			name:           "sum_over_time_24h_window_1h_step",
			query:          `sum_over_time(http_requests_total[1h])`,
			start:          time.Unix(0, 0),
			end:            time.Unix(24*60*60, 0),
			step:           1 * time.Hour,
			description:    "sum_over_time 24h range, 1h window, 1h step (24 evaluations)",
			samplesScanned: 2880 + 120, // 24h + 1h lookback
			evaluations:    24,
			samplesPerEval: 120,
			totalProcessed: 120 * 24, // 2,880
		},
		{
			name:           "sum_over_time_12h_window_30m_step",
			query:          `sum_over_time(http_requests_total[12h])`,
			start:          time.Unix(12*60*60, 0), // 12h
			end:            time.Unix(24*60*60, 0), // 24h
			step:           30 * time.Minute,
			description:    "sum_over_time 12h range, 12h window, 30m step (24 evaluations)",
			samplesScanned: 2880, // 24h (12h + 12h lookback)
			evaluations:    24,
			samplesPerEval: 1440,
			totalProcessed: 1440 * 24, // 34,560
		},
		{
			name:           "sum_over_time_24h_window_2m30s_step",
			query:          `sum_over_time(http_requests_total[24h])`,
			start:          time.Unix(23*60*60, 0), // 23h
			end:            time.Unix(24*60*60, 0), // 24h
			step:           150 * time.Second,      // 2m30s
			description:    "sum_over_time 1h range, 24h window, 2m30s step (24 evaluations)",
			samplesScanned: 2880, // 24h (1h + 24h lookback, but capped at dataset)
			evaluations:    24,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 24, // 69,120
		},
		{
			name:           "histogram_quantile_1h_window_1h_step",
			query:          `histogram_quantile(0.5, rate(http_response_seconds_bucket[1h]))`,
			start:          time.Unix(0, 0),
			end:            time.Unix(24*60*60, 0),
			step:           1 * time.Hour,
			description:    "histogram_quantile 24h range, 1h window, 1h step (24 evaluations)",
			samplesScanned: 2880 + 120, // 24h + 1h lookback
			evaluations:    24,
			samplesPerEval: 120,
			totalProcessed: 120 * 24, // 2,880
		},
		{
			name:           "histogram_quantile_12h_window_30m_step",
			query:          `histogram_quantile(0.5, rate(http_response_seconds_bucket[12h]))`,
			start:          time.Unix(12*60*60, 0), // 12h
			end:            time.Unix(24*60*60, 0), // 24h
			step:           30 * time.Minute,
			description:    "histogram_quantile 12h range, 12h window, 30m step (24 evaluations)",
			samplesScanned: 2880, // 24h (12h + 12h lookback)
			evaluations:    24,
			samplesPerEval: 1440,
			totalProcessed: 1440 * 24, // 34,560
		},
		{
			name:           "histogram_quantile_24h_window_2m30s_step",
			query:          `histogram_quantile(0.5, rate(http_response_seconds_bucket[24h]))`,
			start:          time.Unix(23*60*60, 0), // 23h
			end:            time.Unix(24*60*60, 0), // 24h
			step:           150 * time.Second,      // 2m30s
			description:    "histogram_quantile 1h range, 24h window, 2m30s step (24 evaluations)",
			samplesScanned: 2880, // 24h (1h + 24h lookback, but capped at dataset)
			evaluations:    24,
			samplesPerEval: 2880,
			totalProcessed: 2880 * 24, // 69,120
		},
	}

	opts := engine.Opts{
		EngineOpts: promql.EngineOpts{
			Logger:               nil,
			Reg:                  nil,
			MaxSamples:           50000000,
			Timeout:              100 * time.Second,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ng := engine.New(opts)

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				qry, err := ng.NewRangeQuery(context.Background(), storage, nil, tc.query, tc.start, tc.end, tc.step)
				testutil.Ok(b, err)

				res := qry.Exec(context.Background())
				testutil.Ok(b, res.Err)
			}
		})
	}
}
