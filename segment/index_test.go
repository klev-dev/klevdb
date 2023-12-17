package segment

import (
	"fmt"
	"testing"

	"github.com/klev-dev/klevdb/index"
	"github.com/stretchr/testify/require"
)

func genSegments(offsets ...int64) []Segment[index.OffsetIndex, index.OffsetItem, struct{}, *index.OffsetRuntime] {
	var segments []Segment[index.OffsetIndex, index.OffsetItem, struct{}, *index.OffsetRuntime]
	for _, offset := range offsets {
		segments = append(segments, New[index.OffsetIndex]("", offset))
	}
	return segments
}

func TestConsumeSegment(t *testing.T) {
	var segments = genSegments(500, 1500, 2500, 3500, 4500, 5500)

	var tests = []struct {
		in  int64
		out int64
	}{
		// low bound
		{0, 500},
		{250, 500},
		{500, 500},
		// high bound
		{5500, 5500},
		{6000, 5500},
		// middles
		{1000, 500},
		{1500, 1500},
		{2000, 1500},
		{3000, 2500},
		{4000, 3500},
		{5000, 4500},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%4d|%4d", tc.in, tc.out), func(t *testing.T) {
			s, _ := Consume(segments, tc.in)
			require.Equal(t, tc.out, s.Offset)
		})
	}
}
