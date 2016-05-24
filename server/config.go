package server

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	Inputs	[]map[string]yaml.MapSlice `yaml:"inputs"`
	Outputs []map[string]yaml.MapSlice `yaml:"outputs"`
	Routes	[]map[string]yaml.MapSlice `yaml:"routes"`
}

func LoadConfig(file string) (*Config, error) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("Could not read config file %s: %v", file, err)
	}

	var conf *Config
	err = yaml.Unmarshal(b, &conf)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse config %s: %v", file, err)
	}

	return conf, nil
}
