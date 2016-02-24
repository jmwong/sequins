package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stripe/sequins/backend"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	sequinsVersion string

	bind       = kingpin.Flag("bind", "Address to bind to. Overrides the config option of the same name.").Short('b').Default("localhost:9599").PlaceHolder("ADDRESS").String()
	root       = kingpin.Flag("root", "Where the sequencefiles are. Overrides the config option of the same name.").Short('r').PlaceHolder("URI").String()
	configPath = kingpin.Flag("config", "The config file to use. By default, either sequins.conf in the local directory or /etc/sequins.conf will be used.").PlaceHolder("PATH").String()
)

func main() {
	kingpin.Version("sequins version " + sequinsVersion)
	kingpin.Parse()

	config, err := loadConfig(*configPath)
	if err == errNoConfig {
		// If --root was specified, we can just use that and the default config.
		if *root != "" {
			config = defaultConfig()
		} else {
			log.Fatal("No config file found! Please see the README for instructions.")
		}
	} else if err != nil {
		log.Fatal("Error loading config:", err)
	}

	if *root != "" {
		config.Root = *root
	}

	if *bind != "" {
		config.Bind = *bind
	}

	parsed, err := url.Parse(config.Root)
	if err != nil {
		log.Fatal(err)
	}

	var s *sequins
	switch parsed.Scheme {
	case "", "file":
		s = localSetup(config.Root, config)
	case "s3":
		s = s3Setup(parsed.Host, parsed.Path, config)
	case "hdfs":
		s = hdfsSetup(parsed.Host, parsed.Path, config)
	}

	if config.ZK.Servers != nil {
		err = s.initDistributed()
		if err != nil {
			log.Fatal(err)
		}
	}

	err = s.init()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		log.Fatal(s.start())
	}()

	refresh := config.RefreshPeriod.Duration
	if refresh != 0 {
		ticker := time.NewTicker(refresh)
		go func() {
			log.Println("Automatically checking for new versions every", refresh.String())
			for range ticker.C {
				err = s.reloadLatest()
				if err != nil {
					log.Println(fmt.Errorf("Error reloading: %s", err))
				}
			}
		}()
	}

	sighups := make(chan os.Signal)
	signal.Notify(sighups, syscall.SIGHUP)

	for range sighups {
		err = s.reloadLatest()
		if err != nil {
			log.Println(fmt.Errorf("Error reloading: %s", err))
		}
	}
}

func localSetup(localPath string, config sequinsConfig) *sequins {
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		log.Fatal(err)
	}

	backend := backend.NewLocalBackend(absPath)
	return newSequins(backend, config)
}

func s3Setup(bucketName string, path string, config sequinsConfig) *sequins {
	regionName := config.S3.Region

	// Requiring region name in config for now
	// if regionName == "" {
	// 	regionName = aws.InstanceRegion()
	// 	if regionName == "" {
	// 		log.Fatal("Unspecified S3 region, and no instance region found.")
	// 	}
	// }

	sess := session.New(&aws.Config{
		Region: aws.String(regionName),
		Credentials: credentials.NewStaticCredentials(config.S3.AccessKeyId, config.S3.SecretAccessKey, ""),
	})

	backend := backend.NewS3Backend(bucketName, path, sess)
	return newSequins(backend, config)
}

func hdfsSetup(namenode string, path string, config sequinsConfig) *sequins {
	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatal(fmt.Errorf("Error connecting to HDFS: %s", err))
	}

	backend := backend.NewHdfsBackend(client, namenode, path)
	return newSequins(backend, config)
}
