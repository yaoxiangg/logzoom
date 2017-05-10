package persistent

import (
	"errors"
	"compress/gzip"
	"bufio"
	"fmt"
	"path/filepath"
	"log"
	"strconv"
	"os"
	"io/ioutil"
	"strings"
	"time"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/input"
	"github.com/packetzoom/logzoom/server"
	"github.com/paulbellamy/ratecounter"
	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 10000
)

type Config struct {
	LocalPath     string `yaml:"local_path"`
	FileTag       string `yaml:"file_tag"`
	SampleSize    *int   `yaml:"sample_size,omitempty"`
	GZipEncoded   bool   `yaml:"gzipencoded,omitempty"`
}

type PersistentInputServer struct {
	name     string
	config   Config
	receiver input.Receiver
	term     chan bool
}

func init() {
	input.Register("persistent", New)
}

func New() input.Input {
	return &PersistentInputServer{term: make(chan bool, 1)}
}

func persistentRead(persistentServer *PersistentInputServer) error {
	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	for {
		files, err := ioutil.ReadDir(persistentServer.config.LocalPath)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range files {
			if (time.Since(file.ModTime()).Seconds() > 10.0) {
				if (!strings.Contains(file.Name(), "archive") && strings.Contains(file.Name(), persistentServer.config.FileTag)) {
					log.Println(time.Since(file.ModTime()))
					file, error := os.Open(persistentServer.config.LocalPath+"/"+file.Name())
					if error != nil {
						log.Fatal(error)
					}
					defer file.Close()
					scanner := bufio.NewScanner(file)
					if (persistentServer.config.GZipEncoded) {
						gr, err := gzip.NewReader(file)
						if err != nil {
							log.Fatal(error)
						}
						defer gr.Close();
						scanner = bufio.NewScanner(gr)
					}
					counter := 0
					for scanner.Scan() {
						var ev buffer.Event
						payload := scanner.Text()
						ev.Text = &payload

						if server.RandInt(0, 100) < *persistentServer.config.SampleSize {
							rateCounter.Incr(1)
							persistentServer.receiver.Send(&ev)
						}
						counter += 1
					}
					log.Println("Sent one file: " + file.Name() + " with " + strconv.Itoa(counter) + " records")
					err = os.Rename(file.Name(), persistentServer.config.LocalPath+"/archive/"+filepath.Base(file.Name()))
					if err != nil {
						log.Fatal(err)
					}
				}
			}
		}
		time.Sleep(1000)
	}

	return nil
}

func (persistentServer *PersistentInputServer) ValidateConfig(config *Config) error {
	if len(config.LocalPath) == 0 {
		return errors.New("Missing Persistent input path (local_path)")
	}

	// Create the local path if necessary
	if err := os.MkdirAll(config.LocalPath, 0700); err != nil {
		return errors.New("could not mkdir " + config.LocalPath)
	}

	// Create the local path if necessary
	if err := os.MkdirAll(config.LocalPath+"/archive", 0700); err != nil {
		return errors.New("could not mkdir " + config.LocalPath+"/archive")
	}

	if persistentServer.config.SampleSize == nil {
		i := 100
		persistentServer.config.SampleSize = &i
	}
	log.Printf("[%s] Setting Sample Size to %d", persistentServer.name, *persistentServer.config.SampleSize)

	return nil
}

func (persistentServer *PersistentInputServer) Init(name string, config yaml.MapSlice, receiver input.Receiver) error {
	var persistentConfig *Config

	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &persistentConfig); err != nil {
		return fmt.Errorf("Error parsing Persistent config: %v", err)
	}

	persistentServer.name = name
	persistentServer.config = *persistentConfig
	persistentServer.receiver = receiver

	log.Printf("[%s] Setting config ", persistentServer.config.LocalPath)
	if err := persistentServer.ValidateConfig(persistentConfig); err != nil {
		return fmt.Errorf("Error in config: %v", err)
	}

	return nil
}

func (persistentServer *PersistentInputServer) Start() error {
	log.Printf("Starting Persistent input on directory: %s, File Tag: %s",
		persistentServer.config.LocalPath, persistentServer.config.FileTag)

	go persistentRead(persistentServer)

	for {
		select {
		case <-persistentServer.term:
			log.Println("Persistent input server received term signal")
			return nil
		}
	}

	return nil
}

func (persistentServer *PersistentInputServer) Stop() error {
	persistentServer.term <- true
	return nil
}
