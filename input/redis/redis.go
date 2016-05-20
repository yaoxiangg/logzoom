package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/adjust/redismq"
	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/input"
	"github.com/paulbellamy/ratecounter"
	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Db           int64  `yaml:"db"`
	Password     string `yaml:"password"`
	InputQueue   string `yaml:"input_queue"`
	WorkingQueue string `yaml:"working_queue"`
	JsonDecode   bool   `yaml:"json_decode"`
	LogType    []string `yaml:"type"`
}

type RedisInputServer struct {
	config   Config
	receiver input.Receiver
	term     chan bool
}

func init() {
	input.Register("redis", &RedisInputServer{
		term: make(chan bool, 1),
	})
}

func redisGet(redisServer *RedisInputServer, consumer *redismq.Consumer) error {
	consumer.ResetWorking()
	rateCounter := ratecounter.NewRateCounter(1 * time.Second)
	for {
		unacked := consumer.GetUnackedLength()

		if unacked > 0 {
			log.Printf("Requeued %d messages\n", unacked)
			consumer.RequeueWorking()
		}

		packages, err := consumer.MultiGet(recvBuffer)

		if err == nil {
			numPackages := len(packages)

			if numPackages > 0 {
				rateCounter.Incr(int64(numPackages))
				err = packages[numPackages-1].MultiAck()

				if err != nil {
					log.Println("Failed to ack", err)
				}
			}

			for i := range packages {
				var ev buffer.Event
				payload := string(packages[i].Payload)
				ev.Text = &payload
				if redisServer.config.JsonDecode {
					decoder := json.NewDecoder(strings.NewReader(payload))
					decoder.UseNumber()

					err = decoder.Decode(&ev.Fields)
					if err != nil {
						continue
					}
				}
                if (ev.Fields == nil) {
                    field_map := (make(map[string]interface{}))
                    ev.Fields = &field_map
                }
                (*ev.Fields)["log_type"] = consumer.Name
				redisServer.receiver.Send(&ev)
			}
		} else {
			log.Printf("Error reading from Redis: %s, sleeping", err)
			time.Sleep(2 * time.Second)
		}
	}

	return nil
}

func (redisServer *RedisInputServer) ValidateConfig(config *Config) error {
	if len(config.Host) == 0 {
		return errors.New("Missing Redis host")
	}

	if config.Port <= 0 {
		return errors.New("Missing Redis port")
	}

	if len(config.InputQueue) == 0 {
		return errors.New("Missing Redis input queue name")
	}

	if len(config.WorkingQueue) == 0 {
		return errors.New("Missing Redis working queue name")
	}

	return nil
}

func (redisServer *RedisInputServer) Init(config yaml.MapSlice, receiver input.Receiver) error {
	var redisConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &redisConfig); err != nil {
		return fmt.Errorf("Error parsing Redis config: %v", err)
	}

	if err := redisServer.ValidateConfig(redisConfig); err != nil {
		return fmt.Errorf("Error in config: %v", err)
	}

	redisServer.config = *redisConfig
	redisServer.receiver = receiver

	return nil
}

func (redisServer *RedisInputServer) Start() error {
	log.Printf("Starting Redis input on input queue: %s, working queue: %s",
		redisServer.config.InputQueue,
		redisServer.config.WorkingQueue)

	port := strconv.Itoa(redisServer.config.Port)
    for _, logType := range redisServer.config.LogType {
	    queue := redismq.CreateQueue(redisServer.config.Host,
		    port,
		    redisServer.config.Password,
		    redisServer.config.Db,
		    logType + "_" + redisServer.config.InputQueue)
	    consumer, err := queue.AddConsumer(logType + "_" + redisServer.config.WorkingQueue)
	    if err != nil {
		    log.Println("Error opening Redis input")
		    return nil
	    }
	    go redisGet(redisServer, consumer)
    }

	for {
		select {
		case <-redisServer.term:
			log.Println("Redis input server received term signal")
			return nil
		}
	}

	return nil
}

func (redisServer *RedisInputServer) Stop() error {
	redisServer.term <- true
	return nil
}
