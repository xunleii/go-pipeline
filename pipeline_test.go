package pipeline_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/pipeline"
)

func TestPipeline_Errors(t *testing.T) {
	tts := []struct {
		stages []pipeline.Stage
		expect error
	}{
		{stages: nil, expect: pipeline.ErrEmptyPipeline},
		{stages: []pipeline.Stage{pipeline.P(nil), nil, pipeline.C(nil)}, expect: pipeline.ErrNilStage},
	}

	for _, tt := range tts {
		_, err := pipeline.New(tt.stages...)
		assert.EqualError(t, err, tt.expect.Error())
	}
}
