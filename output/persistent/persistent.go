package persistent

import (
	"crypto/rand"
	"fmt"
	"errors"
//	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"github.com/packetzoom/logzoom/server"

	"github.com/paulbellamy/ratecounter"

	"gopkg.in/yaml.v2"
)

const (
	//New File Every 10sec
	persistentFlushInterval = 10
	recvBuffer             = 100000
	maxSimultaneousWrites = 8
)

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Config struct {
	LocalPath       string `yaml:"local_path"`
//	TimeSliceFormat string `yaml:"time_slice_format"`
	SampleSize      *int   `yaml:"sample_size,omitempty"`
}

type OutputFileInfo struct {
	Filename string
	Count    int
}

type FileSaver struct {
	Config      Config
	Writer      *os.File
	FileInfo    OutputFileInfo
	RateCounter *ratecounter.RateCounter
}

type PersistentWriter struct {
	name          string
	fields        map[string]string
	Config        Config
	Sender        buffer.Sender
	currentUUID   string
	term          chan bool
}

func (fileSaver *FileSaver) WriteToFile(name string, event *buffer.Event) error {
	if fileSaver.Writer == nil {
		log.Println("Creating new Persistent writer")
		file, err := os.Create(fileSaver.Config.LocalPath+"/"+name)

		if err != nil {
			log.Printf("Error creating temporary file:", err)
		}

		fileSaver.Writer = file
		fileSaver.FileInfo.Filename = file.Name()
		fileSaver.FileInfo.Count = 0
	}

	text := *event.Text
	_, err := fileSaver.Writer.Write([]byte(text))

	if err != nil {
		log.Println("Error writing:", err)
		return err
	}

	_, err = fileSaver.Writer.Write([]byte("\n"))

	if err != nil {
		log.Println("Error writing:", err)
		return err
	}

	fileSaver.FileInfo.Count += 1
	fileSaver.RateCounter.Incr(1)

	return nil
}

func init() {
	output.Register("persistent", New)
}

func New() (output.Output) {
	return &PersistentWriter{term: make(chan bool, 1)}
}

func (persistentWriter *PersistentWriter) ValidateConfig(config *Config) error {
	if len(config.LocalPath) == 0 {
		return errors.New("missing local path")
	}

	// Create the local path if necessary
	if err := os.MkdirAll(config.LocalPath, 0700); err != nil {
		return errors.New("could not mkdir " + config.LocalPath)
	}

	if persistentWriter.Config.SampleSize == nil {
		i := 100
		persistentWriter.Config.SampleSize = &i
	}
	log.Printf("[%s] Setting Sample Size to %d", persistentWriter.name, *persistentWriter.Config.SampleSize)

	return nil
}

func (persistentWriter *PersistentWriter) Init(name string, config yaml.MapSlice, sender buffer.Sender, route route.Route) error {
	var persistentConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &persistentConfig); err != nil {
		return fmt.Errorf("Error parsing Persistent config: %v", err)
	}

	persistentWriter.name = name
	persistentWriter.Config = *persistentConfig
	persistentWriter.fields = route.Fields
	persistentWriter.Sender = sender

	if err := persistentWriter.ValidateConfig(persistentConfig); err != nil {
		return fmt.Errorf("Error in config: %v", err)
	}

	log.Println("Done instantiating Persistent Writer")
	return nil
}

func (persistentWriter *PersistentWriter) Start() error {
	if (persistentWriter.Sender == nil) {
		log.Printf("[%s] No route is specified for this output", persistentWriter.name)
		return nil
	}
	// Create file saver
	fileSaver := new(FileSaver)
	fileSaver.Config = persistentWriter.Config
	fileSaver.RateCounter = ratecounter.NewRateCounter(1 * time.Second)

	id := persistentWriter.name
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	persistentWriter.Sender.AddSubscriber(id, receiveChan)
	defer persistentWriter.Sender.DelSubscriber(id)

	tick := time.NewTicker(time.Duration(persistentFlushInterval) * time.Second)

	persistentWriter.currentUUID = uuid()

	for {
		select {
		case ev := <-receiveChan:
			var allowed bool
			allowed = true
			for key, value :=  range persistentWriter.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
					allowed = false
					break
				}
			}
			if allowed && server.RandInt(0, 100) <= *persistentWriter.Config.SampleSize {
				fileSaver.WriteToFile(persistentWriter.name + "_" + persistentWriter.currentUUID, ev)
			}
		case <-tick.C:
			persistentWriter.currentUUID = uuid()
			fileSaver.Writer = nil
		case <-persistentWriter.term:
			log.Println("PersistentWriter received term signal")
			return nil
		}
	}

	return nil
}

func (s *PersistentWriter) Stop() error {
	s.term <- true
	return nil
}
