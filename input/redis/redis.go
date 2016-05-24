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
	JsonDecode   bool   `yaml:"json_decode"`
}

type RedisInputServer struct {
	instance []*RedisInputInstance
}

type RedisInputInstance struct {
	name     string
	config   Config
	receiver input.Receiver
	term     chan bool
}

func init() {
	input.Register("redis", &RedisInputServer{})
}

func redisGet(redisInstance *RedisInputInstance, consumer *redismq.Consumer) error {
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
				if redisInstance.config.JsonDecode {
					decoder := json.NewDecoder(strings.NewReader(payload))
					decoder.UseNumber()

					err = decoder.Decode(&ev.Fields)

					if err != nil {
						continue
					}
				}

				redisInstance.receiver.Send(&ev)
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

	return nil
}

func (redisServer *RedisInputServer) InitInstance(name string, config yaml.MapSlice, receiver input.Receiver) (input.InputInstance, error) {
	var redisConfig *Config
	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &redisConfig); err != nil {
		return nil, fmt.Errorf("Error parsing Redis config: %v", err)
	}

	if err := redisServer.ValidateConfig(redisConfig); err != nil {
		return nil, fmt.Errorf("Error in config: %v", err)
	}

	instance := &RedisInputInstance{name: name, config: *redisConfig, receiver: receiver, term: make(chan bool, 1)}
	redisServer.instance = append(redisServer.instance, instance)
	return instance, nil
}

func (redisInstance *RedisInputInstance) Start() error {
	log.Printf("Starting Redis input on input queue: %s, working queue: %s",
		redisInstance.config.InputQueue,
		redisInstance.config.InputQueue + "_working")

	port := strconv.Itoa(redisInstance.config.Port)

	// Create Redis queue
	queue := redismq.CreateQueue(redisInstance.config.Host,
		port,
		redisInstance.config.Password,
		redisInstance.config.Db,
		redisInstance.config.InputQueue)

	consumer, err := queue.AddConsumer(redisInstance.config.InputQueue + "_working")

	if err != nil {
		log.Println("Error opening Redis input")
		return err
	}

	go redisGet(redisInstance, consumer)

	for {
		select {
		case <-redisInstance.term:
			log.Println("Redis input server received term signal")
			return nil
		}
	}

	return nil
}

func (redisInstance *RedisInputInstance) Stop() error {
	redisInstance.term <- true
	return nil
}

func (redisServer *RedisInputServer) GetNumInstance() int {
	return len(redisServer.instance)
}

