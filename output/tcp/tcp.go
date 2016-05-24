package tcp

import (
	"fmt"
	"log"
	"net"
	"time"
	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host string `yaml:"host"`
}

type TCPServer struct {
	instance []*TCPInstance
}

type TCPInstance struct {
	name string
	fields map[string]string
	host string
	b buffer.Sender
	term chan bool
}

func init() {
	output.Register("tcp", &TCPServer{})
}

// lumberConn handles an incoming connection from a lumberjack client
func (s *TCPInstance) accept(c net.Conn) {
	defer func() {
		s.b.DelSubscriber(s.name)
		log.Printf("[%s - %s] closing tcp connection", s.name, c.RemoteAddr().String())
		c.Close()
	}()

	log.Printf("[%s - %s] accepting tcp connection", s.name, c.RemoteAddr().String())
	// Add the client as a subscriber
	r := make(chan *buffer.Event, recvBuffer)
	s.b.AddSubscriber(s.name, r)

	for {
		select {
		case ev := <-r:
			var allowed bool
			allowed = true
			for key, value :=  range s.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
					allowed = false
					break
				}
                        }
                        if allowed {
				_, err := c.Write([]byte(fmt.Sprintf("%s %s\n", ev.Source, *ev.Text)))
				if err != nil {
					log.Printf("[%s - %s] error sending event to tcp connection: %v", s.name, c.RemoteAddr().String(), err)
					return
				}
			}
		}
	}

}

func (s *TCPServer) InitInstance(name string, config yaml.MapSlice, b buffer.Sender, r route.Route) (output.OutputInstance, error) {
	var tcpConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &tcpConfig); err != nil {
		return nil, fmt.Errorf("Error parsing tcp config: %v", err)
	}
	instance := &TCPInstance{name: name, host: tcpConfig.Host, b: b, fields: r.Fields, term: make(chan bool, 1)}
	s.instance = append(s.instance, instance)
	return instance, nil
}

func (tcpi *TCPInstance) Start() error {
	if (tcpi.b == nil) {
		log.Printf("[%s] No Route is specified for this output", tcpi.name)
		return nil
	}
	tcp_addr, err := net.ResolveTCPAddr("tcp", tcpi.host)
	if err != nil {
		return fmt.Errorf("TCPServer: Failed to parse host address: %v", err)
	}
	ln, err := net.ListenTCP("tcp", tcp_addr)
	ln.SetDeadline(time.Now().Add(5*time.Second))

	if err != nil {
		return fmt.Errorf("TCPServer: listener failed: %v", err)
	}
	log.Printf("[%s - %s] TCP Instance started", tcpi.name, tcpi.host)

	for {
		select {
		case <-tcpi.term:
			log.Println("TCPServer received term signal")
			return nil
		default:
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("Error accepting tcp connection: %v", err)
				continue
			}
			go tcpi.accept(conn)
		}
	}
	return nil
}

func (tcpi *TCPInstance) Stop() error {
	tcpi.term <- true
	return nil
}

func (s *TCPServer) GetNumInstance() int {
	return len(s.instance)
}
