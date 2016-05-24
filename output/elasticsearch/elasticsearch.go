package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"github.com/paulbellamy/ratecounter"
	"gopkg.in/olivere/elastic.v2"
	"gopkg.in/yaml.v2"
)

const (
	defaultHost        = "127.0.0.1"
	defaultIndexPrefix = "logstash"
	esFlushInterval    = 5
	esMaxConns         = 20
	esRecvBuffer       = 100
	esSendBuffer       = 100
)

type Indexer struct {
	bulkService       *elastic.BulkService
	indexPrefix       string
	indexType         string
	RateCounter       *ratecounter.RateCounter
	lastDisplayUpdate time.Time
}

type Config struct {
	Hosts           []string `yaml:"hosts"`
	IndexPrefix     string   `yaml:"index"`
	IndexType       string   `yaml:"index_type"`
	Timeout         int      `yaml:"timeout"`
	GzipEnabled     bool     `yaml:"gzip_enabled"`
	InfoLogEnabled  bool     `yaml:"info_log_enabled"`
	ErrorLogEnabled bool     `yaml:"error_log_enabled"`
}

type ESServer struct {
	instance []*ESInstance
}

type ESInstance struct {
	name   string
	fields map[string]string
	config Config
	host   string
	hosts  []string
	b      buffer.Sender
	term   chan bool
}

func init() {
	output.Register("elasticsearch", &ESServer{})
}

// Dummy discard, satisfies io.Writer without importing io or os.
type DevNull struct{}

func (DevNull) Write(p []byte) (int, error) {
	return len(p), nil
}

func indexName(idx string) string {
	if len(idx) == 0 {
		idx = defaultIndexPrefix
	}

	return fmt.Sprintf("%s-%s", idx, time.Now().Format("2006.01.02"))
}

func (i *Indexer) flush() error {
	numEvents := i.bulkService.NumberOfActions()

	if numEvents > 0 {
		if time.Now().Sub(i.lastDisplayUpdate) >= time.Duration(1*time.Second) {
			log.Printf("Flushing %d event(s) to Elasticsearch, current rate: %d/s", numEvents, i.RateCounter.Rate())
			i.lastDisplayUpdate = time.Now()
		}

		_, err := i.bulkService.Do()

		if err != nil {
			log.Printf("Unable to flush events: %s", err)
		}

		return err
	}

	return nil
}

func (i *Indexer) index(ev *buffer.Event) error {
	doc := *ev.Text
	idx := indexName(i.indexPrefix)
	typ := i.indexType
	request := elastic.NewBulkIndexRequest().Index(idx).Type(typ).Doc(doc)
	i.bulkService.Add(request)
	i.RateCounter.Incr(1)

	numEvents := i.bulkService.NumberOfActions()

	if numEvents < esSendBuffer {
		return nil
	}

	return i.flush()
}

func (e *ESServer) ValidateConfig(config *Config) error {
	if len(config.Hosts) == 0 {
		return errors.New("Missing hosts")
	}

	if len(config.IndexPrefix) == 0 {
		return errors.New("Missing index prefix (e.g. logstash)")
	}

	if len(config.IndexType) == 0 {
		return errors.New("Missing index type (e.g. logstash)")
	}

	return nil
}

func (e *ESServer) InitInstance(name string, config yaml.MapSlice, b buffer.Sender, r route.Route) (output.OutputInstance, error) {
	var esConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &esConfig); err != nil {
		return nil, fmt.Errorf("Error parsing elasticsearch config: %v", err)
	}
	if err := e.ValidateConfig(esConfig); err != nil {
                return nil, fmt.Errorf("Error in config: %v", err)
        }

	instance := &ESInstance{name: name, config: *esConfig, fields: r.Fields, host: fmt.Sprintf("%s:%d", defaultHost, time.Now().Unix()), hosts: esConfig.Hosts, b: b, term: make(chan bool, 1)}
	e.instance = append(e.instance, instance)
	return instance, nil
}

func (ei *ESInstance) readInputChannel(idx *Indexer, receiveChan chan *buffer.Event) {
	// Drain the channel only if we have room
	if idx.bulkService.NumberOfActions() < esSendBuffer {
		select {
		case ev := <-receiveChan:
			var allowed bool
			allowed = true
			for key, value :=  range ei.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
						allowed = false
						break
				}
			}
                        if allowed {
				idx.index(ev)
			}
		case <- ei.term:
			return
		}
	} else {
		log.Printf("Internal Elasticsearch buffer is full, waiting")
		time.Sleep(1 * time.Second)
	}
}

func (ei *ESInstance) insertIndexTemplate(client *elastic.Client) error {
	var template map[string]interface{}
	err := json.Unmarshal([]byte(IndexTemplate), &template)

	if err != nil {
		return err
	}

	template["template"] = ei.config.IndexPrefix + "-*"

	inserter := elastic.NewIndicesPutTemplateService(client)
	inserter.Name(ei.config.IndexPrefix)
	inserter.Create(true)
	inserter.BodyJson(template)

	response, err := inserter.Do()

	if response != nil {
		log.Println("Inserted template response:", response.Acknowledged)
	}

	return err
}

func (ei *ESInstance) Start() error {
	if (ei.b == nil) {
		log.Printf("[%s] No Route is specified for this output", ei.name)
		return nil
	}
	var client *elastic.Client
	var err error
	for {
		httpClient := http.DefaultClient
		timeout := 60 * time.Second

		if ei.config.Timeout > 0 {
			timeout = time.Duration(ei.config.Timeout) * time.Second
		}

		log.Printf("[%s] Setting HTTP timeout to %s", ei.name, timeout)
		log.Printf("[%s] Setting GZIP enabled: %t", ei.name, ei.config.GzipEnabled)

		httpClient.Timeout = timeout

		var infoLogger, errorLogger *log.Logger

		if ei.config.InfoLogEnabled {
			infoLogger = log.New(os.Stdout, "", log.LstdFlags)
		} else {
			infoLogger = log.New(new(DevNull), "", log.LstdFlags)
		}

		if ei.config.ErrorLogEnabled {
			errorLogger = log.New(os.Stderr, "", log.LstdFlags)
		} else {
			errorLogger = log.New(new(DevNull), "", log.LstdFlags)
		}

		client, err = elastic.NewClient(elastic.SetURL(ei.hosts...),
			elastic.SetHttpClient(httpClient),
			elastic.SetGzip(ei.config.GzipEnabled),
			elastic.SetInfoLog(infoLogger),
			elastic.SetErrorLog(errorLogger))

		if err != nil {
			log.Printf("Error starting Elasticsearch: %s, will retry", err)
			time.Sleep(2 * time.Second)
			continue
		}

		ei.insertIndexTemplate(client)

		break
	}
	log.Printf("[%s] Connected to Elasticsearch", ei.name)

	service := elastic.NewBulkService(client)

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, esRecvBuffer)
	ei.b.AddSubscriber(ei.name, receiveChan)
	defer ei.b.DelSubscriber(ei.name)

	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	// Create indexer
	idx := &Indexer{service, ei.config.IndexPrefix, ei.config.IndexType, rateCounter, time.Now()}
	// Loop events and publish to elasticsearch
	tick := time.NewTicker(time.Duration(esFlushInterval) * time.Second)

	for {
		ei.readInputChannel(idx, receiveChan)

		if len(tick.C) > 0 || len(ei.term) > 0 {
			select {
			case <-tick.C:
				idx.flush()
			case <-ei.term:
				tick.Stop()
				log.Println("Elasticsearch received term signal")
				break
			}
		}
	}

	return nil
}

func (ei *ESInstance) Stop() error {
	ei.term <- true
	return nil
}

func (es *ESServer) GetNumInstance() int {
	return len(es.instance)
}

