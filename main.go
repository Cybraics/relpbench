package main

import (

	// "fmt"
	"bufio"
	"crypto/tls"
	"flag"
	"net"
	"os"

	"time"

	"strings"

	"github.com/Cybraics/gorelp"
	log "github.com/sirupsen/logrus"
)

var (
	writers    = flag.Int("writers", 1, "Number of connections and writers to the RELP receiver")
	random     = flag.Bool("random", false, "generate random data rather than reading from stdin")
	host       = flag.String("host", "", "Hostname to connect to")
	doTls      = flag.Bool("tls", false, "Use TLS to connect (unimplemented)")
	bufSize    = flag.Int("bufsize", 100, "Internal channel buffer size")
	windowSize = flag.Int("windowsize", 100, "RELP window size")
	doDrain    = flag.Bool("drain", true, "Wait for outstanding messages to be acked (def true)")
	debug      = false
)

var (
	flushString = "\n__FLUSH__\n"
)

type LineChan chan *string

// Writer reads strings from a channel and writes them to a RELP server
func Writer(c LineChan, exitChan chan int, client relp.Client) error {
	defer func() { exitChan <- 1 }()
	ticker := time.NewTicker(10 * time.Second)
	var line *string
	var more bool
	for {
		// read new string and flush the output channel periodically
		select {
		case line, more = <-c:
			if more {
				client.SendString(*line)
			}
			break
		case <-ticker.C:
			client.Flush()
			break
		}

		if !more {
			if debug {
				log.Debug("Writer exiting due to empty channel")
			}
			if *doDrain {
				_ = client.Drain(30 * time.Second)
			}
			return client.Close()
		}
	}
}

// StartWriter creates a connection and starts a Writer
func StartWriter(c LineChan, exitChan chan int) error {
	conn, err := net.Dial("tcp", *host)
	if err != nil {
		log.Fatal("can't connect to server")
		return err
	}

	if *doTls {
		tlsconfig := tls.Config{
			InsecureSkipVerify: true,
		}
		conn = tls.Client(conn, &tlsconfig)
	}

	eventChan := make(chan relp.ClientEvent, 20)
	go func() {
		for event := range eventChan {
			log.Print("RECEIVED EVENT: ", event)
		}
	}()

	client, err := relp.NewClientFrom(conn, *windowSize, nil)
	if err != nil {
		log.Fatal("can't create or connect to server ", err.Error())
		return err
	}
	return Writer(c, exitChan, client)
}

func copyLines(input *bufio.Reader, output LineChan) error {
	for {
		line, err := input.ReadString('\n')
		if debug {
			log.Info("Got string: ", line)
		}
		trimmed := strings.TrimRight(line, "\n\r")
		if trimmed != "" {
			output <- &trimmed
		}
		// terminate on EOF
		if err != nil {
			return err
		}
	}
}

func main() {
	flag.BoolVar(&debug, "debug", false, "debug mode")
	flag.Parse()

	log.Info("Starting up!")

	pipeline := make(LineChan, *bufSize)
	exitChan := make(chan int)

	for i := 0; i < *writers; i++ {
		go StartWriter(pipeline, exitChan)
	}

	copyLines(bufio.NewReader(os.Stdin), pipeline)

	close(pipeline)

	// wait for all writers to terminate
	for i := 0; i < *writers; i++ {
		if debug {
			log.Info("writer exited")
		}
		<-exitChan
	}
}
