package toydb

import "testing"

func BenchmarkPut(t *testing.B) {
	t.SetParallelism(8)

}