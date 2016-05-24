package s3

import (
	"compress/gzip"
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"

	"github.com/jehiah/go-strftime"
	"github.com/paulbellamy/ratecounter"

	"gopkg.in/yaml.v2"
)

const (
	s3FlushInterval        = 10
	recvBuffer             = 100
	maxSimultaneousUploads = 8
)

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Config struct {
	AwsKeyId    string `yaml:"aws_key_id"`
	AwsSecKey   string `yaml:"aws_sec_key"`
	AwsS3Bucket string `yaml:"aws_s3_bucket"`
	AwsS3Region string `yaml:"aws_s3_region"`

	LocalPath       string `yaml:"local_path"`
	Path            string `yaml:"s3_path"`
	TimeSliceFormat string `yaml:"time_slice_format"`
	AwsS3OutputKey  string `yaml:"aws_s3_output_key"`
}

type OutputFileInfo struct {
	Filename string
	Count	 int
}

type FileSaver struct {
	Config      Config
	Writer      *gzip.Writer
	FileInfo    OutputFileInfo
	RateCounter *ratecounter.RateCounter
}

func (fileSaver *FileSaver) WriteToFile(s3WriterInstance S3WriterInstance, event *buffer.Event) error {
	if fileSaver.Writer == nil {
		log.Println("Creating new S3 gzip writer")
		file, err := ioutil.TempFile(fileSaver.Config.LocalPath, s3WriterInstance.name)

		if err != nil {
			log.Printf("Error creating temporary file:", err)
		}

		fileSaver.Writer = gzip.NewWriter(file)
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

func (s3WriterInstance *S3WriterInstance) doUpload(fileInfo OutputFileInfo) error {
	log.Printf("Opening file %s\n", fileInfo.Filename)
	reader, err := os.Open(fileInfo.Filename)

	if err != nil {
		log.Printf("Failed to open file:", err)
		return err
	}

	curTime := time.Now()
	hostname, _ := os.Hostname()
	timeKey := strftime.Format(s3WriterInstance.Config.TimeSliceFormat, curTime)

	valuesForKey := map[string]string{
		"path":      s3WriterInstance.Config.Path,
		"timeSlice": timeKey,
		"hostname":  hostname,
		"uuid":      uuid(),
	}

	destFile := s3WriterInstance.Config.AwsS3OutputKey

	for key, value := range valuesForKey {
		expr := "%{" + key + "}"
		destFile = strings.Replace(destFile, expr, value, -1)
	}

	result, s3Error := s3WriterInstance.S3Uploader.Upload(&s3manager.UploadInput{
		Body:            reader,
		Bucket:          aws.String(s3WriterInstance.Config.AwsS3Bucket),
		Key:             aws.String(destFile),
		ContentEncoding: aws.String("gzip"),
	})

	if s3Error == nil {
		log.Printf("[%s] %d events written to S3 %s", s3WriterInstance.name, fileInfo.Count, result.Location)
		os.Remove(fileInfo.Filename)
	} else {
		log.Printf("Error uploading to S3", s3Error)
	}

	return s3Error

}

func (s3WriterInstance *S3WriterInstance) WaitForUpload() {
	for {
		select {
		case fileInfo := <-s3WriterInstance.uploadChannel:
			s3WriterInstance.doUpload(fileInfo)
		}
	}
}

func (s3WriterInstance *S3WriterInstance) InitiateUploadToS3(fileSaver *FileSaver) {
	if fileSaver.Writer == nil {
		return
	}

	log.Printf("[%s] Upload to S3, current event rate: %d/s\n", s3WriterInstance.name, fileSaver.RateCounter.Rate())
	writer := fileSaver.Writer
	fileInfo := fileSaver.FileInfo
	fileSaver.Writer = nil
	writer.Close()

	s3WriterInstance.uploadChannel <- fileInfo
}

type S3Writer struct {
	instance      []*S3WriterInstance
}

type S3WriterInstance struct {
	name          string
	fields        map[string]string
	Config        Config
	Sender        buffer.Sender
	S3Uploader    *s3manager.Uploader
	uploadChannel chan OutputFileInfo
	term          chan bool
}

func init() {
	output.Register("s3", &S3Writer{})
}

func (s3Writer *S3Writer) ValidateConfig(config *Config) error {
	if len(config.LocalPath) == 0 {
		return errors.New("missing local path")
	}

	// Create the local path if necessary
	if err := os.MkdirAll(config.LocalPath, 0700); err != nil {
		return errors.New("could not mkdir " + config.LocalPath)
	}

	// Try writing to local path
	if _, err := ioutil.TempFile(config.LocalPath, "logzoom"); err != nil {
		return errors.New("unable to write to " + config.LocalPath)
	}

	if len(config.AwsS3Bucket) == 0 {
		return errors.New("missing AWS S3 bucket")
	}

	if len(config.AwsS3Region) == 0 {
		return errors.New("missing AWS S3 region")
	}

	if len(config.AwsS3OutputKey) == 0 {
		return errors.New("missing AWS S3 output key")
	}

	return nil
}

func (s3Writer *S3Writer) InitInstance(name string, config yaml.MapSlice, sender buffer.Sender, route route.Route) (output.OutputInstance, error) {
	var s3Config *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &s3Config); err != nil {
		return nil, fmt.Errorf("Error parsing S3 config: %v", err)
	}

	if err := s3Writer.ValidateConfig(s3Config); err != nil {
		return nil, fmt.Errorf("Error in config: %v", err)
	}

	aws_access_key_id := (*s3Config).AwsKeyId
	aws_secret_access_key := (*s3Config).AwsSecKey

	token := ""
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()

	if err != nil {
		return nil, err
	}

	session := session.New(&aws.Config{
		Region:      &((*s3Config).AwsS3Region),
		Credentials: creds,
	})

	instance := &S3WriterInstance{name: name, fields: route.Fields, uploadChannel: make(chan OutputFileInfo, maxSimultaneousUploads), Config: *s3Config, Sender: sender, S3Uploader: s3manager.NewUploader(session), term: make(chan bool, 1)}
	s3Writer.instance = append(s3Writer.instance, instance)
	log.Printf("[%s] Done instantiating S3 uploader", name)
	return instance, nil
}

func (s3WriterInstance *S3WriterInstance) Start() error {
	if (s3WriterInstance.Sender == nil) {
		log.Printf("[%s] No route is specified for this output", s3WriterInstance.name)
		return nil
	}
	// Create file saver
	fileSaver := new(FileSaver)
	fileSaver.Config = s3WriterInstance.Config
	fileSaver.RateCounter = ratecounter.NewRateCounter(1 * time.Second)

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	s3WriterInstance.Sender.AddSubscriber(s3WriterInstance.name, receiveChan)
	defer s3WriterInstance.Sender.DelSubscriber(s3WriterInstance.name)

	// Loop events and publish to S3
	tick := time.NewTicker(time.Duration(s3FlushInterval) * time.Second)

	log.Printf("[%s] S3Writer Instance Started", s3WriterInstance.name)
	go s3WriterInstance.WaitForUpload()

	for {
		select {
		case ev := <-receiveChan:
			var allowed bool
			allowed = true
			for key, value :=  range s3WriterInstance.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
					allowed = false
					break
				}
                        }
                        if allowed {
				fileSaver.WriteToFile(*s3WriterInstance, ev)
			}
		case <-tick.C:
			s3WriterInstance.InitiateUploadToS3(fileSaver)
		case <-s3WriterInstance.term:
			log.Println("S3Writer received term signal")
			return nil
		}
	}

	return nil
}

func (s3WriterInstance *S3WriterInstance) Stop() error {
	s3WriterInstance.term <- true
	return nil
}

func (s *S3Writer) GetNumInstance() int {
	return len(s.instance)
}

