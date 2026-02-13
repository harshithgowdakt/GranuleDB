package processor

// Status tells the executor what the processor needs.
type Status int

const (
	// StatusNeedData: input has no data; schedule upstream.
	StatusNeedData Status = iota
	// StatusPortFull: output has unconsumed data; schedule downstream.
	StatusPortFull
	// StatusReady: processor has data to process; call Work().
	StatusReady
	// StatusFinished: processor is done and will never produce more.
	StatusFinished
)

// Processor is a node in the execution DAG.
//
// Prepare() inspects port states and returns what the executor should do.
// It must be lightweight (no heavy computation).
//
// Work() performs the actual computation. Called only when Prepare()
// returned StatusReady. May be called from any goroutine in the pool.
type Processor interface {
	Name() string
	Prepare() Status
	Work()
	Inputs() []*InputPort
	Outputs() []*OutputPort
}

// BaseProcessor provides common port management.
type BaseProcessor struct {
	name    string
	inputs  []*InputPort
	outputs []*OutputPort
}

// NewBaseProcessor creates a BaseProcessor with the given port counts.
func NewBaseProcessor(name string, numInputs, numOutputs int) BaseProcessor {
	inputs := make([]*InputPort, numInputs)
	for i := range inputs {
		inputs[i] = NewInputPort()
	}
	outputs := make([]*OutputPort, numOutputs)
	for i := range outputs {
		outputs[i] = NewOutputPort()
	}
	return BaseProcessor{
		name:    name,
		inputs:  inputs,
		outputs: outputs,
	}
}

func (b *BaseProcessor) Name() string          { return b.name }
func (b *BaseProcessor) Inputs() []*InputPort   { return b.inputs }
func (b *BaseProcessor) Outputs() []*OutputPort  { return b.outputs }

// Input returns the i-th input port.
func (b *BaseProcessor) Input(i int) *InputPort { return b.inputs[i] }

// Output returns the i-th output port.
func (b *BaseProcessor) Output(i int) *OutputPort { return b.outputs[i] }
