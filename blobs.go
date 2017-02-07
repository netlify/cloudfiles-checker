package main

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/spf13/cobra"
)

const blobCol = "blobs"

type blob struct {
	ID  string `bson:"_id"`
	SHA string `bson:"sha"`
}

func searchForBlobs(_ *cobra.Command, _ []string) {
	log.Println("Connecting to mongo")
	sess := connectToMongo()
	db := sess.DB(dbName)
	log.Println("Connected to mongo: " + dbName)

	log.Println("Building cache store")
	store = buildCacheStore(sess, db.C(assetCol))
	work := make(chan *blob)

	wg := sync.WaitGroup{}
	log.Printf("Starting %d workers\n", numWorkers)
	for i := numWorkers; i > 0; i-- {
		wg.Add(1)
		go consumeBlobs(i, &wg, work)
	}

	query := queryForTotal(db.C(blobCol))
	iter := query.Iter()
	obj := new(blob)
	for iter.Next(obj) {
		work <- obj
		obj = new(blob)
	}

	close(work)
	log.Println("Finished enqueuing work - waiting for it to be completed")
	wg.Wait()

	printSummary()
}

func consumeBlobs(id int, wg *sync.WaitGroup, work chan *blob) {
	debug("%d: starting blob consumer", id)
	for b := range work {
		debug("%d: starting to process %d", id, b.ID)
		if !check(b.SHA) {
			bust := busted{Type: "blob", ID: b.ID, Digest: b.SHA}
			brokenLock.Lock()
			broken = append(broken, bust)
			brokenLock.Unlock()

			log.Printf("BROKEN FILE DISCOVERED: blob: %s, sha: %s\n", b.ID, b.SHA)
		}
		debug("%d: finished processing %d", id, b.ID)

		numDone := atomic.AddInt64(&objectsCompleted, 1)
		if numDone%100 == 0 {
			log.Printf("Completed %d of %d\n", numDone, objectsTotal)
		}
	}
	debug("%d: shutting down consumer", id)
	wg.Done()
}
