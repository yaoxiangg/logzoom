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
	"github.com/paulbellamy/ratecounter"

	"gopkg.in/yaml.v2"
)

const (
	redisFlushInterval  = 5
	rateDisplayInterval = 10
	recvBuffer          = 100
)

type Config struct {
	Host       string   `yaml:"host"`
	Port       int      `yaml:"port"`
	Db         int64    `yaml:"db"`
	Password   string   `yaml:"password"`
	CopyQueues []string `yaml:"copy_queues"`
	LogType    []string `yaml:"type"`
}

type RedisServer struct {
	config Config
	sender buffer.Sender
	term   chan bool
}

type QueueData struct {
    data    string
    index   int
}

type RedisQueue struct {
	queueList  []*redismq.BufferedQueue
	data   chan QueueData
	term   chan bool
	ticker time.Ticker
    name   string
}

func NewRedisQueue(config Config, key string) *RedisQueue {
	port := strconv.Itoa(config.Port)
	queueList := []*redismq.BufferedQueue{}
    for _, logType := range config.LogType {
        queue := redismq.CreateBufferedQueue(config.Host,
		    port,
	    	config.Password,
		    config.Db,
		    logType + "_" + key,
		    recvBuffer)
 	    queue.Start()
        queueList = append(queueList, queue)
    }
	return &RedisQueue{queueList: queueList,
		data:   make(chan QueueData),
		term:   make(chan bool),
		ticker: *time.NewTicker(time.Duration(redisFlushInterval) * time.Second),
        name:   key}
}

func (redisQueue *RedisQueue) insertToRedis(index int, text string) error {
    err := redisQueue.queueList[index].Put(text)
	if err != nil {
		fmt.Println("Error inserting data: ", err)
		return err
	}

	if len(redisQueue.queueList[index].Buffer) > recvBuffer {
		redisQueue.queueList[index].FlushBuffer()
	}

	return nil
}

func (redisQueue *RedisQueue) flushQueue() error {
    for _, queue := range redisQueue.queueList {
	    if len(queue.Buffer) > 0 {
		    log.Printf("Flushing %d events to Redis", len(queue.Buffer))
	    }
	    queue.FlushBuffer()
	}
    return nil
}

func (redisQueue *RedisQueue) Start() {
	for {
		select {
		case text := <-redisQueue.data:
			redisQueue.insertToRedis(text.index, text.data)
		case <-redisQueue.ticker.C:
			redisQueue.flushQueue()
		case <-redisQueue.term:
			redisQueue.flushQueue()
		}
	}

}

func init() {
	output.Register("redis", &RedisServer{
		term: make(chan bool, 1),
	})
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

func (redisServer *RedisServer) Init(config yaml.MapSlice, sender buffer.Sender) error {
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
	redisServer.sender = sender
	return nil
}

func (redisServer *RedisServer) Start() error {
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	redisServer.sender.AddSubscriber(redisServer.config.Host, receiveChan)
	defer redisServer.sender.DelSubscriber(redisServer.config.Host)

	allQueues := make([]*RedisQueue, len(redisServer.config.CopyQueues))

	// Create Redis queue
	for index, key := range redisServer.config.CopyQueues {
		redisQueue := NewRedisQueue(redisServer.config, key)
		allQueues[index] = redisQueue
		go redisQueue.Start()
	}

	// Loop events and publish to Redis
	tick := time.NewTicker(time.Duration(redisFlushInterval) * time.Second)
	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

    index_map := map[string]int{}
    for index, logType := range redisServer.config.LogType {
            index_map[logType] = index
    }
	for {
		select {
		case ev := <-receiveChan:
			rateCounter.Incr(1)
            log_type := (*ev.Fields)["log_type"].(string)
			text := *ev.Text
			for _, queue := range allQueues {
			    queue.data <- QueueData{data: text, index: index_map[log_type]}
			}
		case <-tick.C:
			if rateCounter.Rate() > 0 {
				log.Printf("Current Redis input rate: %d/s\n", rateCounter.Rate())
			}
		case <-redisServer.term:
			log.Println("RedisServer received term signal")
			for _, queue := range allQueues {
				queue.term <- true
            }

			return nil
		}
	}

	return nil
}

func (s *RedisServer) Stop() error {
	s.term <- true
	return nil
}
