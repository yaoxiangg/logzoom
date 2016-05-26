package server

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"gopkg.in/yaml.v2"
	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/input"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
)

type Server struct {
	Config *Config
	buffers map[string]*buffer.Buffer

	mtx	sync.Mutex
	inputs	map[string]input.Input
	outputs map[string]output.Output
	routes	map[string]route.Route
}

func signalCatcher() chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	return c
}

func New(configFile string) (*Server, error) {
	config, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	return &Server{
		Config:  config,
		buffers: make(map[string]*buffer.Buffer),
		inputs:  make(map[string]input.Input),
		outputs: make(map[string]output.Output),
		routes:  make(map[string]route.Route),
	}, nil
}

func (s *Server) Start() {
	log.Println("Starting server")


	s.mtx.Lock()

	// Init inputs
	for _, inputEntry := range s.Config.Inputs {
		for name, inputConfig := range inputEntry {
			for i, item := range inputConfig {
				if i > 0 {
					panic("There are more than one configuration specified for an input entry.")
				}
				if i == 0 { //There should be only 1 input per entry
					in, err := input.Load(item.Key.(string))
					if err != nil {
						log.Println(err.Error)
						continue
					}
					err = in.Init(name, item.Value.(yaml.MapSlice));
					if err != nil {
						log.Fatalf("Failed to init %s input: %v", item.Key, err)
					}
					s.inputs[name] = in
				}
			}
		}
	}
	// Init outputs
	for _, outputEntry := range s.Config.Outputs {
		for name, outputConfig := range outputEntry {
			for i, item := range outputConfig {
				if i > 0 {
					panic("There are more than one configuration specified for an output entry.")
				}
				if i == 0 { //There should be only 1 output per entry
					out, err := output.Load(item.Key.(string))
					if err != nil {
						log.Println(err.Error)
						continue
					}
					err = out.Init(name, item.Value.(yaml.MapSlice));
					if err != nil {
						log.Fatalf("Failed to init %s output: %v", item.Key, err)
					}
					s.outputs[name] = out
				}
			}
		}
	}

	// Parse routes and Start buffer
	log.Println("Starting buffer")
	for _, routeEntry := range s.Config.Routes {
		for name, routeDetails := range routeEntry {
			var input string
			var output string
			rules := make(map[string]string)
			for _, item := range routeDetails {
				if item.Key.(string) == "input" {
					input = item.Value.(string)
				}
				if item.Key.(string) == "output" {
					output = item.Value.(string)
				}
				if item.Key.(string) == "rules" {
					for _, rule := range item.Value.(yaml.MapSlice) {
						rules[rule.Key.(string)] = rule.Value.(string)
					}
				}
			}
			if (&input != nil && &output != nil) {
				s.buffers[input] = buffer.New()
				go s.buffers[input].Start()
				route := route.Route{Input: input, Output: output, Fields: rules}
				s.routes[name] = route
			}
		}
	}

	// Join Inputs and Outputs to Routes
	for route_name, value := range s.routes {
		if s.inputs[value.Input] != nil {
			err := s.inputs[value.Input].Join(s.buffers[value.Input]);
			if err != nil {
				log.Fatalf("Failed to Join %s input: %v", value.Input, err)
			}
		}
		if s.outputs[value.Output] != nil {
			err := s.outputs[value.Output].Join(s.buffers[value.Input], s.routes[route_name]);
			if err != nil {
				log.Fatalf("Failed to Join %s output: %v", value.Output, err)
			}
		}
	}

	// Start Inputs
	for name, in := range s.inputs {
		go func(name string, instance input.Input) {
			if err := instance.Start(); err != nil {
				log.Fatalf("Error starting input %s: %v", name, err)
			}
		} (name, in)
	}

	// Start Outputs
	for name, out := range s.outputs {
		go func(name string, instance output.Output) {
			if err := instance.Start(); err != nil {
				log.Fatalf("Error starting output %s: %v", name, err)
			}
		} (name, out)
	}

	s.mtx.Unlock()

	// Wait for kill signal
	<-signalCatcher()
	log.Printf("Received quit signal")

	// Stop Server
	s.Stop()
}

func (s *Server) Stop() {
	log.Println("Stopping server")

	s.mtx.Lock()

	// stop inputs
	for name, in := range s.inputs {
		log.Printf("Stopping input %s", name)
		if err := in.Stop(); err != nil {
			log.Printf("Error stopping %s input: %v", name, err)
		}
	}

	// stop ouputs
	for name, out := range s.outputs {
		log.Printf("Stopping output %s", name)
		if err := out.Stop(); err != nil {
			log.Printf("Error stopping %s output: %v", name, err)
		}
	}

	s.mtx.Unlock()

	for name, buffer := range s.buffers {
		log.Printf("Stopping buffer for input: %s", name)
		if err := buffer.Stop(); err != nil {
			log.Printf("Error stopping %s buffer: %v", name, err)
		}
	}
}
