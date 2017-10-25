package main

import (

	// "fmt"
	"bufio"
	"crypto/tls"
	"flag"
	"net"
	"os"

	"time"

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

// Writer reads strings from a channel and writes them to a RELP server
func Writer(c chan string, exitChan chan int, client relp.Client) error {
	defer func() { exitChan <- 1 }()
	for {
		line, more := <-c
		if !more {
			if debug {
				log.Debug("Writer exiting due to empty channel")
			}
			if *doDrain {
				_ = client.Drain(30 * time.Second)
			}
			return client.Close()
		}
		client.SendString(line)
	}
}

// StartWriter creates a connection and starts a Writer
func StartWriter(c chan string, exitChan chan int) error {
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

	client, err := relp.NewClientFrom(conn, *windowSize)
	if err != nil {
		log.Fatal("can't create or connect to server ", err.Error())
		return err
	}
	return Writer(c, exitChan, client)
}

func copyLines(input *bufio.Reader, output chan string) error {
	for {
		line, err := input.ReadString('\n')
		if line != "" {
			if debug {
				log.Info("Got string: ", line)
			}
			output <- line
		}
		if err != nil {
			return err
		}
	}
}

func main() {
	flag.BoolVar(&debug, "debug", false, "debug mode")
	flag.Parse()

	log.Info("Starting up!")

	pipeline := make(chan string, *bufSize)
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
