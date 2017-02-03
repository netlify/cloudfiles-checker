package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/spf13/cobra"
)

const (
	treeCol  = "trees"
	assetCol = "private-assets"
)

var servers []string
var dbName string
var useTLS bool
var caFiles []string
var certFile string
var keyFile string

var sinceStr string
var verbose bool
var numWorkers int

var store BlobStore
var region string
var user string
var pass string
var container string
var internal bool

var treesCompleted int64
var treesTotal int
var broken []busted
var brokenLock sync.Mutex
var checked = make(map[string]bool)

type tree struct {
	ID      string `bson:"_id"`
	Entries map[string]entry
}

type entry struct {
	Digest string `bson:"d"`
	Size   int    `bson:"s"`
	Type   string `bson:"t"`
}

type busted struct {
	TreeID string
	Digest string
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		log.Fatal("Failed to execute command: " + err.Error())
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Run: run,
	}

	root.Flags().StringSliceVarP(&servers, "server", "s", []string{"mongo.lo"}, "which mongo instance to connect to")
	root.Flags().StringSliceVar(&caFiles, "ca", []string{"/etc/cfssl/certs/ca.pem"}, "a ca file to use")
	root.Flags().StringVar(&certFile, "cert", "/etc/cfssl/certs/cert.pem", "a cert file to use")
	root.Flags().StringVar(&keyFile, "key", "/etc/cfssl/certs/cert-key.pem", "a key file to use")
	root.Flags().StringVar(&dbName, "db", "bitballoon", "the name of the db to use")
	root.Flags().StringVarP(&user, "user", "u", "netlify", "the name of the user to use for cloudfiles")
	root.Flags().StringVarP(&pass, "pass", "p", "", "the name of the password to use for cloudfiles")
	root.Flags().StringVarP(&region, "region", "r", "ORD", "the region to use for cloudfiles")
	root.Flags().StringVarP(&container, "container", "c", "private", "the region to use for cloudfiles")
	root.Flags().BoolVar(&internal, "internal", false, "if the cloud files connection is internal")
	root.Flags().StringVarP(&sinceStr, "since", "t", "20h", "a since time in the format of #[m | h | s | d]")
	root.Flags().BoolVar(&useTLS, "tls", false, "if we should use tls")
	root.Flags().BoolVarP(&verbose, "verbose", "v", false, "if we should use tls")
	root.Flags().IntVarP(&numWorkers, "workers", "w", 100, "the number of workers to start")

	return root
}

func run(cmd *cobra.Command, _ []string) {
	var err error

	since := parseSince()

	log.Println("Connecting to mongo")
	sess := connectToMongo()
	db := sess.DB(dbName)
	log.Println("Connected to mongo: " + dbName)

	log.Println("Building cache store")
	store = buildCacheStore(sess, db.C(assetCol))

	work := make(chan *tree)
	wg := sync.WaitGroup{}
	log.Printf("Starting %d workers\n", numWorkers)
	for i := numWorkers; i > 0; i-- {
		wg.Add(1)
		go consume(i, &wg, work)
	}

	log.Println("Going fetching all the tree objects since " + sinceStr + " ago (" + since.String() + ")")
	treeQuery := db.C(treeCol).Find(bson.M{"created_at": bson.M{"$gt": since}})
	treesTotal, err = treeQuery.Count()
	if err != nil {
		log.Fatal("Failed to find out how many trees we are going to process: " + err.Error())
	}
	log.Printf("Discovered %d trees that we need to check\n", treesTotal)

	treeIter := treeQuery.Iter()
	t := new(tree)
	for treeIter.Next(t) {
		work <- t
		t = new(tree)
	}
	close(work)
	log.Println("Finished enqueuing work - waiting for it to be completed")
	wg.Wait()
}

func consume(id int, wg *sync.WaitGroup, work chan *tree) {
	for t := range work {
		debug("%d: starting to process tree: %s", id, t.ID)

		for path, entry := range t.Entries {
			if entry.Size == -1 || entry.Type == "f" || checked[entry.Digest] {
				continue
			}
			checked[entry.Digest] = true

			if !check(entry.Digest) {
				b := busted{TreeID: t.ID, Digest: entry.Digest}
				brokenLock.Lock()
				broken = append(broken, b)
				brokenLock.Unlock()

				log.Printf("BROKEN FILE DISCOVERED: tree: %s, sha: %s, name: %s\n", t.ID, entry.Digest, path)
			}
		}
		debug("%d: finished processing tree: %s", id, t.ID)
		numDone := atomic.AddInt64(&treesCompleted, 1)
		if numDone%100 == 0 {
			log.Printf("Completed %d of %d\n", numDone, treesTotal)
		}
	}
	wg.Done()
}

func check(sha string) bool {
	_, err := store.Get(sha)
	return err == nil
}

func debug(fmt string, args ...interface{}) {
	if verbose {
		log.Printf(fmt+"\n", args...)
	}
}

func parseSince() time.Time {
	sinceStr = strings.ToLower(sinceStr)
	number, err := strconv.Atoi(sinceStr[:len(sinceStr)-1])
	if err != nil {
		log.Fatal("Failed to convert the since time to a number: " + err.Error())
	}
	number = number * -1

	interval := string(sinceStr[len(sinceStr)-1])
	now := time.Now()
	switch interval {
	case "h":
		return now.Add(time.Duration(number) * time.Hour)
	case "m":
		return now.Add(time.Duration(number) * time.Minute)
	case "s":
		return now.Add(time.Duration(number) * time.Second)
	case "d":
		return now.Add(time.Duration(number) * time.Hour * 24)
	}

	log.Fatal("Only understand 'h', 'm', 's', and 'd' - not " + interval)
	return time.Time{}
}

func connectToMongo() *mgo.Session {
	info := &mgo.DialInfo{
		Addrs:   servers,
		Timeout: time.Second * 10,
	}

	if useTLS {
		log.Println("Enabling TLS connection")
		info.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsConfig())
		}
	}

	sess, err := mgo.DialWithInfo(info)
	if err != nil {
		log.Fatal("Failed to dial Mongo: " + err.Error())
	}

	return sess
}

func buildCacheStore(sess *mgo.Session, col *mgo.Collection) BlobStore {
	mongoStore := NewMongoStore(sess, col)
	cfStore := NewCloudfilesStore(user, pass, container, region, internal)
	return NewCacheStore(mongoStore, cfStore)
}

func tlsConfig() *tls.Config {
	pool := x509.NewCertPool()
	for _, caFile := range caFiles {
		caData, err := ioutil.ReadFile(caFile)
		if err != nil {
			log.Fatal("Failed to read ca file " + caFile + ": " + err.Error())
		}

		if !pool.AppendCertsFromPEM(caData) {
			log.Fatal(fmt.Sprintf("Failed to add CA cert at %s", caFile))
		}
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatal("Failed to load the key pair: " + err.Error())
	}

	tlsConfig := &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	return tlsConfig
}
