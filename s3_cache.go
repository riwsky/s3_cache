package main

import (
	"container/list"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
)

// A getResult represents an entry in the cache
type getResult struct {
	keyName          string
	status           string
	localPath        *string
	bucketName       string
	bytesTransferred int64
	md5              string
}

func (r *getResult) MarshalJSON() ([]byte, error) {
	out := map[string]interface{}{"key_name": r.keyName,
		"status":     r.status,
		"local_path": r.localPath}
	return json.Marshal(out)
}

type KeyGetter interface {
	get(bucketName string, keyNames []string) []getResult
}

type CachedKeyGetter interface {
	KeyGetter
	has(bucketName string, keyName string) bool
	remove(bucketName string, keyName string) bool
}

type s3Conn struct {
	*s3.S3
}

type keyReaderGetter interface {
	getKeyReader(bucketName, keyName string) (io.ReadCloser, error)
}

func (s *s3Conn) getKeyReader(bucketName, keyName string) (io.ReadCloser, error) {
	bucket := s.Bucket(bucketName)
	return bucket.GetReader(keyName)
}

type tempKeyGetter struct {
	keyReaderGetter
}

func (t *tempKeyGetter) getKey(bucketName, keyName string) getResult {
	result := getResult{keyName: keyName}
	rc, err := t.getKeyReader(bucketName, keyName)
	defer rc.Close()
	if err != nil {
		result.status = err.Error()
		return result
	}
	f, err := ioutil.TempFile(os.TempDir(), "s3cache_")
	defer f.Close()
	if err != nil {
		result.status = err.Error()
		return result
	}
	h := md5.New()
	written, err := io.Copy(io.MultiWriter(f, h), rc)
	if err != nil {
		result.status = err.Error()
		return result
	}

	localPath := f.Name()
	result.status = fmt.Sprintf("cache miss, transferred %v bytes", written)
	result.localPath = &localPath
	result.bytesTransferred = written
	result.md5 = hex.Dump(h.Sum(nil))
	return result
}

func (t *tempKeyGetter) get(bucketName string, keyNames []string) []getResult {
	out := make([]getResult, 0, len(keyNames))
	inbox := make(chan getResult)

	for _, keyName := range keyNames {
		go func(keyName string) {
			inbox <- t.getKey(bucketName, keyName)
		}(keyName)
	}

	for _ = range keyNames {
		result := <-inbox
		result.bucketName = bucketName
		out = append(out, result)
	}

	return out
}

type lruCachedKeyGetter struct {
	base  KeyGetter
	cache map[string]map[string]*list.Element
	list.List
	sync.RWMutex
}

type boundedDiskCachedKeyGetter struct {
	lru        *lruCachedKeyGetter
	disk       CachedKeyGetter
	downloaded chan int64
}

func (b *boundedDiskCachedKeyGetter) keepClean(maxBytes int64) {
	var totalDownloaded int64 = 0
	for {
		totalDownloaded += <-b.downloaded
		if totalDownloaded > maxBytes {
			b.lru.Lock()
			for maxBytes-totalDownloaded > 0 {
				oldestResult := b.lru.oldest()
				if oldestResult == nil {
					log.Printf("Above maxBytes %v with size of %v, but no entries left in lru!", maxBytes, totalDownloaded)
					break
				}
				b.lru.remove(oldestResult.bucketName, oldestResult.keyName)
				b.disk.remove(oldestResult.bucketName, oldestResult.keyName)
				totalDownloaded -= oldestResult.bytesTransferred
			}
			b.lru.Unlock()
		}
	}
}

func (b *boundedDiskCachedKeyGetter) get(bucketName string, keyNames []string) []getResult {
	out := make([]getResult, len(keyNames))
	known := make([]string, len(keyNames)/2)
	missing := make([]string, len(keyNames)/2)
	for _, keyName := range keyNames {
		if b.lru.has(bucketName, keyName) {
			known = append(known, keyName)
		} else {
			missing = append(missing, keyName)
		}
	}
	out = append(out, b.lru.get(bucketName, known)...)
	var newdled int64
	for _, result := range b.lru.get(bucketName, missing) {
		newdled += result.bytesTransferred
		out = append(out, result)
	}
	b.downloaded <- newdled
	return out
}

type diskCachedKeyGetter struct {
	base     KeyGetter
	cacheDir string
}

func (d *diskCachedKeyGetter) remove(bucketName string, keyName string) bool {
	err := os.Remove(d.pathFor(bucketName, keyName))
	return !os.IsNotExist(err)
}

func (m *lruCachedKeyGetter) oldest() *getResult {
	if m.List.Len() == 0 {
		return nil
	}
	result := m.List.Back().Value.(getResult)
	return &result
}

func (m *lruCachedKeyGetter) remove(bucketName string, keyName string) bool {
	if !m.has(bucketName, keyName) {
		return false
	} else {
		m.Lock()
		m.Remove(m.cache[bucketName][keyName])
		delete(m.cache[bucketName], keyName)
		m.Unlock()
		return true
	}
}

func (m *lruCachedKeyGetter) get(bucketName string, keyNames []string) []getResult {
	bucket, had := m.cache[bucketName]
	if !had {
		m.Lock()
		bucket = make(map[string]*list.Element, 2*len(keyNames))
		m.cache[bucketName] = bucket
		m.Unlock()
	}
	out := make([]getResult, len(keyNames))
	missing := make([]string, len(keyNames)/2)
	for _, keyName := range keyNames {
		m.RLock()
		cachedResultElement, had := bucket[keyName]
		if had {
			m.MoveToFront(cachedResultElement)
			cachedResult := cachedResultElement.Value.(getResult)
			cachedResult.status = "cache_hit"
			out = append(out, cachedResult)
			m.RUnlock()
			continue
		} else {
			m.RUnlock()
			missing = append(missing, keyName)
		}
	}
	if len(missing) > 0 {
		results := m.base.get(bucketName, missing)
		m.Lock()
		for _, result := range results {
			resultElem := m.PushFront(result)
			bucket[result.keyName] = resultElem
			out = append(out, result)
		}
		m.Unlock()
	}
	return out
}

func (m *lruCachedKeyGetter) has(bucketName, keyName string) bool {
	bucket, had := m.cache[bucketName]
	if !had {
		return false
	}
	_, had = bucket[keyName]
	if !had {
		return false
	}
	return true
}

func (d *diskCachedKeyGetter) has(bucketName, keyName string) bool {
	if _, err := os.Stat(d.pathFor(bucketName, keyName)); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}

func (d *diskCachedKeyGetter) get(bucketName string, keyNames []string) []getResult {
	out := make([]getResult, 0, len(keyNames))
	missing := make([]string, 0, len(keyNames)/2)
	for _, keyName := range keyNames {
		var result getResult
		if d.has(bucketName, keyName) {
			localPath := d.pathFor(bucketName, keyName)
			result = getResult{status: "disk cache hit", localPath: &localPath, keyName: keyName,
				bucketName: bucketName}
			out = append(out, result)
		} else {
			missing = append(missing, keyName)
		}
	}
	if len(missing) > 0 {
		results := d.base.get(bucketName, missing)
		for _, result := range results {
			cachedResult, err := d.moveToCache(bucketName, result)
			if err != nil {
				cachedResult.status = err.Error()
				cachedResult.localPath = nil
			}
			out = append(out, cachedResult)
		}
	}
	return out
}

func (d *diskCachedKeyGetter) pathFor(bucketName, keyName string) string {
	return path.Join(d.cacheDir, bucketName, keyName)
}

func (d *diskCachedKeyGetter) moveToCache(bucketName string, g getResult) (getResult, error) {
	newPath := d.pathFor(bucketName, g.keyName)
	if g.localPath == nil {
		return g, fmt.Errorf("no localPath for given getResult")
	}
	if err := os.MkdirAll(path.Dir(newPath), 0777); err != nil {
		return g, fmt.Errorf("couldn't create directory to move getResult to")
	}
	err := os.Rename(*g.localPath, newPath)
	if err != nil {
		return g, err
	}
	g.localPath = &newPath
	return g, nil
}

// An EvictingMutableKeyGetter checks with a ShouldEvicter to determine
// if a key should be deleted from the cache for mutable requests.
// As this exposes the underlying CachedKeyGetter, eviction can be ignored
// by using the .get(bucketName, keyNames) interface
type EvictingMutableKeyGetter struct {
	CachedKeyGetter
	ShouldEvicter
}

type ShouldEvicter interface {
	ShouldEvict(getResult) (bool, error)
}

type md5ShouldEvicter struct {
	*s3.S3
}

func md5For(conn *s3.S3, bucketName, keyName string) (string, error) {
	listResp, err := conn.Bucket(bucketName).List("", "", keyName, 1)
	if err != nil {
		return "", err
	}
	etag := listResp.Contents[0].ETag
	return etag[1 : len(etag)-1], nil
}

func (m *md5ShouldEvicter) ShouldEvict(r getResult) (bool, error) {
	currentMD5, err := md5For(m.S3, r.bucketName, r.keyName)
	if err != nil {
		return false, err
	}
	if r.md5 == currentMD5 {
		return false, nil
	} else {
		return true, nil
	}
}

func (e *EvictingMutableKeyGetter) Get(bucketName string, keyNames []string, mutableBucket bool) []getResult {
	presents := make([]string, 0)
	absents := make([]string, 0, len(keyNames))
	for _, keyName := range keyNames {
		if e.has(bucketName, keyName) {
			presents = append(presents, keyName)
		} else {
			absents = append(absents, keyName)
		}
	}
	cached := e.get(bucketName, presents)
	out := make([]getResult, len(keyNames))
	for _, getResult := range cached {
		if !mutableBucket {
			out = append(out, getResult)
			continue
		}
		evict, err := e.ShouldEvict(getResult)
		if err != nil && !evict {
			out = append(out, getResult)
		} else {
			e.remove(bucketName, getResult.keyName)
			absents = append(absents, getResult.keyName)
		}
	}

	if len(absents) > 0 {
		fetcht := e.get(bucketName, absents)
		for _, getResult := range fetcht {
			out = append(out, getResult)
		}
	}

	return out
}

type MutableKeyGetter interface {
	Get(bucketName string, keyNames []string, mutableBucket bool) []getResult
}

type keyServer struct {
	MutableKeyGetter
}

type CacheRequest struct {
	BucketName    string   `json:"bucket_name"`
	KeyNames      []string `json:"keynames"`
	MutableBucket bool     `json:"mutable_bucket"`
}

func (s *keyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var cr CacheRequest
	err := json.NewDecoder(r.Body).Decode(&cr)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}
	out, err := json.Marshal(s.Get(cr.BucketName, cr.KeyNames, cr.MutableBucket))
	if err != nil {
		http.Error(w, err.Error(), 500)
	}
	w.Write(out)
}

func main() {
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Panicln(err)
	}
	conn := s3.New(auth, aws.USEast)
	s3Conn := s3Conn{conn}
	tempDirGetter := &tempKeyGetter{&s3Conn}
	diskCachedGetter := &diskCachedKeyGetter{base: tempDirGetter}
	evicter := md5ShouldEvicter{conn}
	mutableGetter := EvictingMutableKeyGetter{diskCachedGetter, &evicter}
	server := keyServer{&mutableGetter}
	http.Handle("/", &server)
	http.ListenAndServe(":8780", nil)
}
