package segment

import (
	"fmt"

	"github.com/klev-dev/klevdb/message"
)

type Offsetter interface {
	GetOffset() int64
}

func Consume[S ~[]O, O Offsetter](segments S, offset int64) (O, int) {
	switch offset {
	case message.OffsetOldest:
		return segments[0], 0
	case message.OffsetNewest:
		return segments[len(segments)-1], len(segments) - 1
	}

	beginIndex := 0
	beginSegment := segments[beginIndex]
	if offset <= beginSegment.GetOffset() {
		return beginSegment, beginIndex
	}

	endIndex := len(segments) - 1
	endSegment := segments[endIndex]
	if endSegment.GetOffset() <= offset {
		return endSegment, endIndex
	}

	for beginIndex < endIndex {
		midIndex := (beginIndex + endIndex) / 2
		midSegment := segments[midIndex]
		switch {
		case midSegment.GetOffset() < offset:
			beginIndex = midIndex + 1
		case midSegment.GetOffset() > offset:
			endIndex = midIndex - 1
		default:
			return midSegment, midIndex
		}
	}

	if segments[beginIndex].GetOffset() > offset {
		return segments[beginIndex-1], beginIndex - 1
	}
	return segments[beginIndex], beginIndex
}

var ErrOffsetRelative = fmt.Errorf("%w: get relative offset", message.ErrInvalidOffset)
var ErrOffsetBeforeStart = fmt.Errorf("%w: offset before start", message.ErrNotFound)

func Get[S ~[]O, O Offsetter](segments S, offset int64) (O, int, error) {
	switch offset {
	case message.OffsetOldest:
		return segments[0], 0, nil
	case message.OffsetNewest:
		return segments[len(segments)-1], len(segments) - 1, nil
	}

	beginIndex := 0
	beginSegment := segments[beginIndex]
	switch {
	case offset < beginSegment.GetOffset():
		var v O
		if beginSegment.GetOffset() == 0 {
			return v, -1, ErrOffsetRelative
		}
		return v, -1, ErrOffsetBeforeStart
	case offset == beginSegment.GetOffset():
		return beginSegment, 0, nil
	}

	endIndex := len(segments) - 1
	endSegment := segments[endIndex]
	if endSegment.GetOffset() <= offset {
		return endSegment, endIndex, nil
	}

	for beginIndex < endIndex {
		midIndex := (beginIndex + endIndex) / 2
		midSegment := segments[midIndex]
		switch {
		case midSegment.GetOffset() < offset:
			beginIndex = midIndex + 1
		case midSegment.GetOffset() > offset:
			endIndex = midIndex - 1
		default:
			return midSegment, midIndex, nil
		}
	}

	if segments[beginIndex].GetOffset() > offset {
		return segments[beginIndex-1], beginIndex - 1, nil
	}
	return segments[beginIndex], beginIndex, nil
}
