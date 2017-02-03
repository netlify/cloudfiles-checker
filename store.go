package main

import (
	"bytes"
	"io"
	"log"
	"time"

	"github.com/ncw/swift"

	"gopkg.in/mgo.v2"
)

// -------------------------------------------------------------------------------------------------------------------
// THIS IS STOLEN FROM THE NETLIFY SERVER
// -------------------------------------------------------------------------------------------------------------------
type BlobStore interface {
	Get(string) (io.ReadCloser, error)
}

// CacheStore is a BlobStore that writes through one store to another
type CacheStore struct {
	cache BlobStore
	store BlobStore
}

// NewCacheStore creates a new store from two stores
func NewCacheStore(cache, store BlobStore) *CacheStore {
	cacheStore := &CacheStore{
		cache: cache,
		store: store,
	}
	return cacheStore
}

// Get an object
func (s *CacheStore) Get(filename string) (io.ReadCloser, error) {
	obj, err := s.cache.Get(filename)
	if err == nil {
		return obj, nil
	}
	return s.store.Get(filename)
}

// --------------------------------------------------------------------------------------------------------------------
// MongoStore is a BlobStore backed by MongoDB
// --------------------------------------------------------------------------------------------------------------------
type MongoStore struct {
	collection *mgo.Collection
	session    *mgo.Session
}

// NewMongoStore creates a new mongo store
func NewMongoStore(sess *mgo.Session, coll *mgo.Collection) *MongoStore {
	return &MongoStore{
		session:    sess,
		collection: coll,
	}
}

// MongoObj is an object in the mongostore
type MongoObj struct {
	ID        string `bson:"_id"`
	Content   []byte `bson:"content"`
	Expire    *int64 `bson:"expire"`
	CreatedAt int64  `bson:"created_at"`
}

// Body is the actual response body from an object in the mongo store
type Body struct {
	buffer *bytes.Reader
}

// Read - implementing ReadCloser for Body
func (b *Body) Read(p []byte) (int, error) {
	return b.buffer.Read(p)
}

// Close - implementing ReadCloser for Body
func (b *Body) Close() error {
	return nil
}

// Get an object
func (s *MongoStore) Get(filename string) (io.ReadCloser, error) {
	s.session.Refresh()
	var obj MongoObj
	err := s.collection.FindId(filename).One(&obj)
	if err != nil {
		return nil, err
	}
	return &Body{bytes.NewReader(obj.Content)}, nil
}

// --------------------------------------------------------------------------------------------------------------------
// CloudfilesStore persists blobs in a Rackspace Cloudfiles bucket
// --------------------------------------------------------------------------------------------------------------------
type CloudfilesStore struct {
	conn      *swift.Connection
	container string
}

func NewCloudfilesStore(user, password, container, region string, internal bool) *CloudfilesStore {
	store := CloudfilesStore{
		conn: &swift.Connection{
			UserName:       user,
			ApiKey:         password,
			AuthUrl:        "https://identity.api.rackspacecloud.com/v2.0",
			Region:         region,
			Internal:       internal,
			ConnectTimeout: 60 * time.Second,
		},
		container: container,
	}
	if err := store.conn.Authenticate(); err != nil {
		log.Fatalf("Error authenticating with Cloudfiles: %v (%v:%v)", err, user, password)
	}
	return &store
}

// Get a file
func (s *CloudfilesStore) Get(filename string) (io.ReadCloser, error) {
	var obj *swift.ObjectOpenFile

	var err error
	obj, _, err = s.conn.ObjectOpen(s.container, filename, true, swift.Headers{})
	if err != nil {
		return nil, err
	}

	return NewFileObject(obj)
}

type FileObject struct {
	obj    *swift.ObjectOpenFile
	loc    int64
	length int64
}

// Read delegates to the ObjectOpenFile unless the location is equal to the length.
// In that case we simply return 0 bytes
func (f *FileObject) Read(p []byte) (n int, err error) {
	if f.loc == f.length {
		return 0, nil
	}
	return f.obj.Read(p)
}

// Close the ObjectOpenFile
func (f *FileObject) Close() error {
	return f.obj.Close()
}

// NewFileObject wraps an ObjectOpenFile
func NewFileObject(obj *swift.ObjectOpenFile) (f *FileObject, err error) {
	f = &FileObject{obj: obj, loc: 0}
	f.length, err = obj.Length()
	return f, err
}
