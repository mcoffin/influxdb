package coordinator

import (
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

type MergeChannelProcessor struct {
	next engine.Processor
	c    chan (<-chan *protocol.Response)
	e    chan error
}

func NewMergeChannelProcessor(next engine.Processor, concurrency int) *MergeChannelProcessor {
	p := &MergeChannelProcessor{
		next: next,
		e:    make(chan error, concurrency),
		c:    make(chan (<-chan *protocol.Response), concurrency),
	}
	for i := 0; i < concurrency; i++ {
		p.e <- nil
	}
	return p
}

func (p *MergeChannelProcessor) Close() {
	close(p.c)
	close(p.e)
	for c := range p.c {
	nextChannel:
		for r := range c {
			switch r.GetType() {
			case protocol.Response_END_STREAM,
				protocol.Response_ACCESS_DENIED,
				protocol.Response_WRITE_OK,
				protocol.Response_HEARTBEAT:
				break nextChannel
			}
		}
	}
}

func (p *MergeChannelProcessor) NextChannel(bs int) (chan<- *protocol.Response, error) {
	err := <-p.e
	if err != nil {
		return nil, err
	}
	c := make(chan *protocol.Response, bs)
	p.c <- c
	return c, nil
}

func (p *MergeChannelProcessor) ProcessChannels() {
	for channel := range p.c {
		for response := range channel {
			switch response.GetType() {

			case protocol.Response_WRITE_OK,
				protocol.Response_HEARTBEAT,
				protocol.Response_ACCESS_DENIED,
				protocol.Response_END_STREAM:

				// all these four types end the stream
				if m := response.ErrorMessage; m != nil {
					p.e <- common.NewQueryError(common.InvalidArgument, *m)
				}
				return
			case protocol.Response_QUERY:
				p.next.Yield(response.Series)
			}
		}
	}
}
