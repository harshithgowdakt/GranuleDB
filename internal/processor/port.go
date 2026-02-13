package processor

import "sync/atomic"

// PortState represents the state of a connected port pair.
type PortState uint32

const (
	PortIdle     PortState = iota // no data, no demand
	PortNeedData                  // downstream wants data
	PortHasData                   // upstream produced data
	PortFinished                  // no more data will ever come
)

// portData is shared between a connected OutputPort and InputPort.
// Access is synchronized via atomic operations on state. The chunk field
// is written only when transitioning to HasData and read only when
// transitioning from HasData, so there is no data race.
type portData struct {
	state atomic.Uint32
	chunk *Chunk
}

// OutputPort is the output side of a processor.
type OutputPort struct {
	data *portData
}

// NewOutputPort creates a disconnected output port.
func NewOutputPort() *OutputPort {
	return &OutputPort{data: &portData{}}
}

// Push places a chunk into the port. Returns false if not in NeedData state.
func (p *OutputPort) Push(chunk *Chunk) bool {
	if PortState(p.data.state.Load()) != PortNeedData {
		return false
	}
	p.data.chunk = chunk
	p.data.state.Store(uint32(PortHasData))
	return true
}

// SetFinished signals that this output will never produce more data.
func (p *OutputPort) SetFinished() {
	p.data.state.Store(uint32(PortFinished))
	p.data.chunk = nil
}

// CanPush returns true if the downstream is ready for data.
func (p *OutputPort) CanPush() bool {
	return PortState(p.data.state.Load()) == PortNeedData
}

// IsFinished returns true if the port is in the Finished state.
func (p *OutputPort) IsFinished() bool {
	return PortState(p.data.state.Load()) == PortFinished
}

// InputPort is the input side of a processor.
type InputPort struct {
	data *portData
}

// NewInputPort creates a disconnected input port.
func NewInputPort() *InputPort {
	return &InputPort{data: &portData{}}
}

// Pull extracts the chunk from the port. Returns nil if not in HasData state.
func (p *InputPort) Pull() *Chunk {
	if PortState(p.data.state.Load()) != PortHasData {
		return nil
	}
	chunk := p.data.chunk
	p.data.chunk = nil
	p.data.state.Store(uint32(PortNeedData))
	return chunk
}

// SetNeeded signals that this input wants data.
func (p *InputPort) SetNeeded() {
	if PortState(p.data.state.Load()) == PortIdle {
		p.data.state.Store(uint32(PortNeedData))
	}
}

// HasData returns true if a chunk is available for pulling.
func (p *InputPort) HasData() bool {
	return PortState(p.data.state.Load()) == PortHasData
}

// IsFinished returns true if the upstream has signaled no more data.
func (p *InputPort) IsFinished() bool {
	return PortState(p.data.state.Load()) == PortFinished
}

// IsNeeded returns true if this port is in NeedData state.
func (p *InputPort) IsNeeded() bool {
	return PortState(p.data.state.Load()) == PortNeedData
}

// SetFinished marks this input as finished (used for early cancellation,
// e.g. when LimitProcessor has enough rows).
func (p *InputPort) SetFinished() {
	p.data.state.Store(uint32(PortFinished))
	p.data.chunk = nil
}

// Connect links an OutputPort to an InputPort by sharing the same portData.
// Initial state is NeedData to signal immediate demand.
func Connect(output *OutputPort, input *InputPort) {
	shared := &portData{}
	shared.state.Store(uint32(PortNeedData))
	output.data = shared
	input.data = shared
}
