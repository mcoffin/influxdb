package cluster

import "github.com/influxdb/influxdb/protocol"

type ResponseChannelProcessor struct {
	r ResponseChannel
}

var QueryResponse = protocol.Response_QUERY
var EndStreamResponse = protocol.Response_END_STREAM

func NewResponseChannelProcessor(r ResponseChannel) *ResponseChannelProcessor {
	return &ResponseChannelProcessor{r}
}

func (p *ResponseChannelProcessor) Yield(s *protocol.Series) (bool, error) {
	ok := p.r.Yield(&protocol.Response{
		Type:        &QueryResponse,
		MultiSeries: []*protocol.Series{s},
	})
	return ok, nil
}

func (p *ResponseChannelProcessor) Close() error {
	return nil
}

func (p *ResponseChannelProcessor) Name() string {
	return "ResponseChannelProcessor"
}
