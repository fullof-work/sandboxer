package diffstore

import (
	"sort"
	"testing"
)

func reportSamples(b *testing.B, samples []int64) {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	b.ReportMetric(float64(samples[len(samples)/2])/1000, "p50-us")
	b.ReportMetric(float64(samples[len(samples)*99/100])/1000, "p99-us")
}
