package redis

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adjust/redismq"
	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"github.com/paulbellamy/ratecounter"

	"gopkg.in/yaml.v2"
)

const (
	redisFlushInterval  = 5
	rateDisplayInterval = 10
	recvBuffer	    = 100
)

type Config struct {
	Host       string   `yaml:"host"`
	Port       int      `yaml:"port"`
	Db         int64    `yaml:"db"`
	Password   string   `yaml:"password"`
	CopyQueues []string `yaml:"copy_queues"`
}

type RedisServer struct {
	instance []*RedisInstance
}

type RedisQueue struct {
	queue  *redismq.BufferedQueue
	data   chan string
	term   chan bool
	ticker time.Ticker
}

type RedisInstance struct {
	name   string
	config Config
	fields map[string]string
	sender buffer.Sender
	term   chan bool
}

func NewRedisQueue(config Config, key string) *RedisQueue {
	port := strconv.Itoa(config.Port)

	queue := redismq.CreateBufferedQueue(config.Host,
		port,
		config.Password,
		config.Db,
		key,
		recvBuffer)
	queue.Start()

	return &RedisQueue{queue: queue,
		data:   make(chan string),
		term:   make(chan bool),
		ticker: *time.NewTicker(time.Duration(redisFlushInterval) * time.Second)}
}

func (redisQueue *RedisQueue) insertToRedis(text string) error {
	err := redisQueue.queue.Put(text)

	if err != nil {
		fmt.Println("Error inserting data: ", err)
		return err
	}

	if len(redisQueue.queue.Buffer) > recvBuffer {
		return redisQueue.flushQueue()
	}

	return nil
}

func (redisQueue *RedisQueue) flushQueue() error {
	if len(redisQueue.queue.Buffer) > 0 {
		log.Printf("[%s] Flushing %d events to Redis", redisQueue.queue.Queue.Name, len(redisQueue.queue.Buffer))
	}

	redisQueue.queue.FlushBuffer()
	return nil
}

func (redisQueue *RedisQueue) Start() {
	for {
		select {
		case text := <-redisQueue.data:
			redisQueue.insertToRedis(text)
		case <-redisQueue.ticker.C:
			redisQueue.flushQueue()
		case <-redisQueue.term:
			redisQueue.flushQueue()
		}
	}

}

func init() {
	output.Register("redis", &RedisServer{})
}

func (redisServer *RedisServer) ValidateConfig(config *Config) error {
	if len(config.Host) == 0 {
		return errors.New("Missing Redis host")
	}

	if config.Port <= 0 {
		return errors.New("Missing Redis port")
	}

	if len(config.CopyQueues) == 0 {
		return errors.New("Missing Redis output queues")
	}

	return nil
}

func (redisServer *RedisServer) InitInstance(name string, config yaml.MapSlice, sender buffer.Sender, route route.Route) (output.OutputInstance, error) {
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

	instance := &RedisInstance{name: name, config: *redisConfig, fields: route.Fields, sender: sender, term: make(chan bool, 1)}
	redisServer.instance = append(redisServer.instance, instance)
	return instance, nil
}

func (redisInstance *RedisInstance) Start() error {
	if (redisInstance.sender == nil) {
		log.Printf("[%s] No Route is specified for this output", redisInstance.name)
		return nil
	}
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	redisInstance.sender.AddSubscriber(redisInstance.name, receiveChan)
	defer redisInstance.sender.DelSubscriber(redisInstance.name)

	allQueues := make([]*RedisQueue, len(redisInstance.config.CopyQueues))

	// Create Redis queue
	for index, key := range redisInstance.config.CopyQueues {
		redisQueue := NewRedisQueue(redisInstance.config, key)
		allQueues[index] = redisQueue
		go redisQueue.Start()
	}
	log.Printf("[%s] Started Redis Output Instance", redisInstance.name)
	// Loop events and publish to Redis
	tick := time.NewTicker(time.Duration(redisFlushInterval) * time.Second)
	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	for {
		select {
		case ev := <-receiveChan:
			rateCounter.Incr(1)
			var allowed bool
			allowed = true
			for key, value :=  range redisInstance.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
					allowed = false
					break
				}
			}
			if allowed {
				text := *ev.Text
				for _, queue := range allQueues {
					queue.data <- text
				}
			}
		case <-tick.C:
			if rateCounter.Rate() > 0 {
				log.Printf("[%s] Current Redis input rate: %d/s\n", redisInstance.name, rateCounter.Rate())
			}
		case <-redisInstance.term:
			log.Println("Output RedisServer received term signal")
			for _, queue := range allQueues {
				queue.term <- true
			}

			return nil
		}
	}

	return nil
}

func (s *RedisInstance) Stop() error {
	s.term <- true
	return nil
}

func (s *RedisServer) GetNumInstance() int {
	return len(s.instance)
}

