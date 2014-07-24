package main

import (
	"container/list"
    "crypto/md5"
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

type getResult struct {
	keyName          string
	status           string
	localPath        *string
	bucketName       string
	bytesTransferred int64
    md5 string
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
	conn *s3.S3
}

func (s *s3Conn) getKey(bucket *s3.Bucket, keyName string) getResult {
	result := getResult{keyName: keyName}
	rc, err := bucket.GetReader(keyName)
	defer rc.Close()
	if err != nil {
		result.status = err.Error()
		return result
	}
	f, err := ioutil.TempFile(os.TempDir(), "s3cache_")
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

	localPath := path.Join(os.TempDir(), f.Name())
	result.status = fmt.Sprintf("cache miss, transferred %v bytes", written)
	result.localPath = &localPath
	result.bytesTransferred = written
    result.md5 = string(h.Sum(nil))
	return result
}

func (s s3Conn) get(bucketName string, keyNames []string) []getResult {
	bucket := s.conn.Bucket(bucketName)
	out := make([]getResult, len(keyNames))
	inbox := make(chan getResult)

	for _, keyName := range keyNames {
		go func(keyName string) {
			inbox <- s.getKey(bucket, keyName)
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
			for maxBytes - totalDownloaded > 0 {
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
	base KeyGetter
}

func (d *diskCachedKeyGetter) remove(bucketName string, keyName string) bool {
	err := os.Remove(pathFor(bucketName, keyName))
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
	if _, err := os.Stat(pathFor(bucketName, keyName)); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}

func (d *diskCachedKeyGetter) get(bucketName string, keyNames []string) []getResult {
	out := make([]getResult, len(keyNames))
	missing := make([]string, len(keyNames)/2)
	for _, keyName := range keyNames {
		var result getResult
		if d.has(bucketName, keyName) {
			localPath := pathFor(bucketName, keyName)
			result = getResult{status: "disk cache hit", localPath: &localPath, keyName: keyName}
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
				log.Println(err)
				cachedResult.status = err.Error()
				cachedResult.localPath = nil
			}

			out = append(out, cachedResult)
		}
	}
	return out
}

func pathFor(bucketName, keyName string) string {
	return fmt.Sprintf("/tmp/%v/%v", bucketName, keyName)
}

func (d *diskCachedKeyGetter) moveToCache(bucketName string, g getResult) (getResult, error) {
	newPath := pathFor(bucketName, g.keyName)
	if g.localPath == nil {
		return g, fmt.Errorf("no localPath for given getResult")
	}
	err := os.Rename(*g.localPath, newPath)
	if err != nil {
		return g, err
	}
	g.localPath = &newPath
	g.status = "cache hit"
	return g, nil
}

type md5ValidatingKeyGetter struct {
    CachedKeyGetter
    *s3.S3
}

func (m *md5ValidatingKeyGetter) get(bucketName string, keyNames []string) {
}

type keyServer struct {
	KeyGetter
}

func (s *keyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	vs := r.Form
	bucketName := vs["bucket_name"][0]
	keyNames := vs["key_names"]
	out, err := json.Marshal(s.get(bucketName, keyNames))
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
	baseGetter := s3Conn{conn}
	diskCachedGetter := &diskCachedKeyGetter{base: baseGetter}
	cache := make(map[string]map[string]*list.Element, 100)
	lruCachedGetter := &lruCachedKeyGetter{cache: cache, base: diskCachedGetter}
	downloadedChan := make(chan int64, 25)
	boundedGetter := boundedDiskCachedKeyGetter{lru: lruCachedGetter, disk: diskCachedGetter, downloaded: downloadedChan}
    go boundedGetter.keepClean(10*1024*1024) // 10 MB
	server := keyServer{&boundedGetter}
	http.Handle("/", &server)
	http.ListenAndServe(":8780", nil)
}
