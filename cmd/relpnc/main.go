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
	listener    = flag.String("listener", "localhost:3333", "Host and port for TCP listener")
	tlsListener = flag.String("tls-listener", "localhost:3334", "Host and port for TLS listener (unimpl)")
	bufSize     = flag.Int("bufsize", 100, "Internal channel buffer size")
	windowSize  = flag.Int("windowsize", 100, "RELP window size")
	debug       = false
)

var (
	flushString = "\n__FLUSH__\n"
)

type LineChan chan *string

// Writer reads strings from a channel and writes them to a RELP server
func Writer(c LineChan, exitChan chan int, client relp.Client) error {
	defer func() { exitChan <- 1 }()

	// lush every 10 seconds
	ticker := time.NewTicker(10 * time.Second)

	more := true
	for {
		// read new string and flush the output channel periodically
		select {
		case line, more := <-c:
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
