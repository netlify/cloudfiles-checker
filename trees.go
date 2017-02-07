package main

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/spf13/cobra"
)

const treeCol = "trees"

type tree struct {
	ID      string `bson:"_id"`
	Entries map[string]entry
}

type entry struct {
	Digest string `bson:"d"`
	Size   int    `bson:"s"`
	Type   string `bson:"t"`
}

func searchForTrees(cmd *cobra.Command, _ []string) {
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
		go consumeTrees(i, &wg, work)
	}

	iter := queryForTotal(db.C(treeCol)).Iter()
	obj := new(tree)
	for iter.Next(obj) {
		work <- obj
		obj = new(tree)
	}

	close(work)
	log.Println("Finished enqueuing work - waiting for it to be completed")
	wg.Wait()
	printSummary()
}

func consumeTrees(id int, wg *sync.WaitGroup, work chan *tree) {
	for t := range work {
		debug("%d: starting to process tree: %s", id, t.ID)

		for path, entry := range t.Entries {
			if entry.Size == -1 || entry.Type == "f" || checked[entry.Digest] {
				continue
			}
			checked[entry.Digest] = true

			if !check(entry.Digest) {
				b := busted{Type: "tree", ID: t.ID, Digest: entry.Digest}
				brokenLock.Lock()
				broken = append(broken, b)
				brokenLock.Unlock()

				log.Printf("BROKEN FILE DISCOVERED: tree: %s, sha: %s, name: %s\n", t.ID, entry.Digest, path)
				if single {
					debug("%d: Quitting checking further entries in %d\n", id, t.ID)
					break
				}
			}
		}
		debug("%d: finished processing tree: %s", id, t.ID)
		numDone := atomic.AddInt64(&objectsCompleted, 1)
		if numDone%100 == 0 {
			log.Printf("Completed %d of %d\n", numDone, objectsTotal)
		}
	}
	debug("%d: shutting down consumer", id)
	wg.Done()
}
