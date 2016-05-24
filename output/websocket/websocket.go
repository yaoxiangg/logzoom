package websocket

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"golang.org/x/net/websocket"

	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host string `yaml:"host"`
}

type WebSocketServer struct {
	instance []*WebSocketInstance
}

type WebSocketInstance struct {
	name string
	fields map[string]string
	host string
	b    buffer.Sender
	logs map[string]time.Time
	mtx  sync.RWMutex
	term chan bool
}

var (
	indexTemplate, _ = template.New("index").Parse(index)
	logsTemplate, _  = template.New("logs").Parse(logs)
)

func init() {
	output.Register("websocket", &WebSocketServer{})
}

func (wsi *WebSocketInstance) wslogsHandler(w *websocket.Conn) {
	source := w.Request().FormValue("source")
	host := fmt.Sprintf("%s/%d", w.RemoteAddr().String(), time.Now().UnixNano())

	defer func() {
		log.Printf("[%s] closing websocket conn", w.RemoteAddr().String())
		wsi.b.DelSubscriber(host)
		w.Close()
	}()

	log.Printf("[%s - %s] accepting websocket conn", wsi.name, w.RemoteAddr().String())

	r := make(chan *buffer.Event, recvBuffer)
	wsi.b.AddSubscriber(wsi.name, r)

	for {
		select {
		case ev := <-r:
			if len(source) > 0 {
				if ev.Source != source {
					continue
				}
			}
			//Add rule check here
			var allowed bool
			allowed = true
			for key, value :=  range wsi.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
					allowed = false
					break
				}
                        }
                        if allowed {
				err := websocket.Message.Send(w, *ev.Text)
				if err != nil {
					log.Printf("[%s - %s] error sending ws message: %v", wsi.name, w.RemoteAddr().String(), err.Error())
					return
				}
			}
		}
	}
}

func (wsi *WebSocketInstance) logsHandler(w http.ResponseWriter, r *http.Request) {
	source := "*"
	host := fmt.Sprintf("ws://%s/wslogs", r.Host)

	if src := r.FormValue("source"); len(src) > 0 {
		source = src
		host = fmt.Sprintf("%s?source=%s", host, src)
	}

	logsTemplate.Execute(w, struct{ Source, Server string }{source, host})
}

func (wsi *WebSocketInstance) indexHandler(w http.ResponseWriter, r *http.Request) {
	wsi.mtx.RLock()
	defer wsi.mtx.RUnlock()
	indexTemplate.Execute(w, wsi.logs)
}

func (wsi *WebSocketInstance) logListMaintainer() {
	defer func() {
		wsi.b.DelSubscriber(wsi.name + "_logList")
	}()

	r := make(chan *buffer.Event, recvBuffer)
	wsi.b.AddSubscriber(wsi.name + "_logList", r)

	ticker := time.NewTicker(time.Duration(600) * time.Second)

	log.Printf("[%s] WebSocket Instance Started", wsi.name)
	for {
		select {
		case ev := <-r:
			wsi.mtx.Lock()
			wsi.logs[ev.Source] = time.Now()
			wsi.mtx.Unlock()
		case <-ticker.C:
			t := time.Now()
			wsi.mtx.Lock()
			for log, ttl := range wsi.logs {
				if t.Sub(ttl).Seconds() > 600 {
					delete(wsi.logs, log)
				}
			}
			wsi.mtx.Unlock()
		}
	}
}

func (ws *WebSocketServer) InitInstance(name string, config yaml.MapSlice, b buffer.Sender, r route.Route) (output.OutputInstance, error) {
	var wsConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &wsConfig); err != nil {
		return nil, fmt.Errorf("Error parsing websocket config: %v", err)
	}
	instance := &WebSocketInstance{name: name, host: wsConfig.Host, b: b, fields: r.Fields, logs: make(map[string]time.Time), term: make(chan bool, 1)}
	ws.instance = append(ws.instance, instance)
	return instance, nil
}

func (wsi *WebSocketInstance) Start() error {
	if (wsi.b == nil) {
		log.Printf("[%s] No route is specified for this output", wsi.name)
		return nil
	}
	http.Handle("/wslogs", websocket.Handler(wsi.wslogsHandler))
	http.HandleFunc("/logs", wsi.logsHandler)
	http.HandleFunc("/", wsi.indexHandler)

	go wsi.logListMaintainer()

	err := http.ListenAndServe(wsi.host, nil)
	if err != nil {
		return fmt.Errorf("Error starting websocket server: %v", err)
	}

	return nil
}

func (wsi *WebSocketInstance) Stop() error {
	wsi.term <- true
	return nil
}

func (ws *WebSocketServer) GetNumInstance() int {
	return len(ws.instance)
}

