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
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/spf13/cobra"
)

const assetCol = "private-assets"

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
var single bool

var objectsCompleted int64
var objectsTotal int64

var broken []busted
var brokenLock sync.Mutex
var checked = make(map[string]bool)

type busted struct {
	Type   string
	ID     string
	Digest string
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		log.Fatal("Failed to execute command: " + err.Error())
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{}

	root.PersistentFlags().StringSliceVarP(&servers, "server", "s", []string{"mongo.lo"}, "which mongo instance to connect to")
	root.PersistentFlags().StringSliceVar(&caFiles, "ca", []string{"/etc/cfssl/certs/ca.pem"}, "a ca file to use")
	root.PersistentFlags().StringVar(&certFile, "cert", "/etc/cfssl/certs/cert.pem", "a cert file to use")
	root.PersistentFlags().StringVar(&keyFile, "key", "/etc/cfssl/certs/cert-key.pem", "a key file to use")
	root.PersistentFlags().StringVar(&dbName, "db", "bitballoon", "the name of the db to use")
	root.PersistentFlags().StringVarP(&user, "user", "u", "netlify", "the name of the user to use for cloudfiles")
	root.PersistentFlags().StringVarP(&pass, "pass", "p", "", "the name of the password to use for cloudfiles")
	root.PersistentFlags().StringVarP(&region, "region", "r", "ORD", "the region to use for cloudfiles")
	root.PersistentFlags().StringVarP(&container, "container", "c", "private", "the region to use for cloudfiles")
	root.PersistentFlags().BoolVar(&internal, "internal", false, "if the cloud files connection is internal")
	root.PersistentFlags().StringVarP(&sinceStr, "since", "t", "20h", "a since time in the format of #[m | h | s | d]")
	root.PersistentFlags().BoolVar(&useTLS, "tls", false, "if we should use tls")
	root.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "if we should use tls")
	root.PersistentFlags().BoolVar(&single, "single", false, "if we should quit checking entries after a single failure in a tree")
	root.PersistentFlags().IntVarP(&numWorkers, "workers", "w", 100, "the number of workers to start")

	treeCmd := &cobra.Command{
		Run: searchForTrees,
		Use: "trees",
	}
	blobCmd := &cobra.Command{
		Run: searchForBlobs,
		Use: "blobs",
	}
	root.AddCommand(treeCmd, blobCmd)

	return root
}

func printSummary() {
	fmt.Printf("Discovered %d broken references\n", len(broken))
	fmt.Println("index\ttype\tid\tsha")
	for i, b := range broken {
		fmt.Printf("%d\t%s\t%s\t%s\n", i, b.Type, b.ID, b.Digest)
	}
}

func queryForTotal(col *mgo.Collection) *mgo.Query {
	since := parseSince()

	// query for blobs
	log.Println("Going fetching all the blob objects since " + sinceStr + " ago (" + since.String() + ")")
	query := col.Find(bson.M{"created_at": bson.M{"$gt": since}})
	total, err := query.Count()
	if err != nil {
		log.Fatal("Failed to find out how many objects we are going to process: " + err.Error())
	}
	log.Printf("Discovered %d objects that we need to check\n", total)
	objectsTotal = int64(total)
	return query
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
