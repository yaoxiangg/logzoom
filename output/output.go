package output

import (
	"fmt"
	"gopkg.in/yaml.v2"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/route"
)

type Output interface {
	InitInstance(string, yaml.MapSlice, buffer.Sender, route.Route) (OutputInstance, error)
	GetNumInstance() int
}

type OutputInstance interface {
	Start() error
	Stop() error
}

var (
	outputs = make(map[string]Output)
)

func Register(name string, output Output) error {
	if _, ok := outputs[name]; ok {
		return fmt.Errorf("Output %s already exists", name)
	}
	outputs[name] = output
	return nil
}

func Load(name string) (Output, error) {
	output, ok := outputs[name]
	if !ok {
		return nil, fmt.Errorf("Output %s not found", name)
	}
	return output, nil
}
