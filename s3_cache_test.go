package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

type mockKeyReaderGetter []byte

type mockReadCloser struct {
	io.Reader
}

func (m mockReadCloser) Close() error {
	return nil
}

func (m mockKeyReaderGetter) getKeyReader(bucketName, keyName string) (io.ReadCloser, error) {
	var r io.Reader
	r = bytes.NewReader([]byte(m))
	return mockReadCloser{r}, nil
}

func compareContents(expected, path string, t *testing.T) {
	actualContents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(actualContents, []byte(expected)) != 0 {
		t.Logf("Downloaded file was %v, but expected %v", actualContents, expected)
		t.Fail()
	}
}

func TestTempKeyGetter(t *testing.T) {
	contents := []byte("fancy s3 key contents")
	var kg KeyGetter
	kg = &tempKeyGetter{mockKeyReaderGetter(contents)}
	results := kg.get("bucket", []string{"key1"})
	t.Log(results)
	result := results[0]
	if result.localPath == nil {
		t.Fatal("Didn't return any path for local file")
	}
	defer os.Remove(*result.localPath)
	expectedByteLen := int64(len(contents))
	if result.bytesTransferred != expectedByteLen {
		t.Logf("Transferred %v bytes, but expected %v", result.bytesTransferred, expectedByteLen)
		t.Fail()
	}
	compareContents(string(contents), *result.localPath, t)

	if !strings.Contains(result.status, "cache miss") {
		t.Logf("expected cache miss in the status, had %v", result.status)
		t.Fail()
	}
}

type mockKeyGetter struct {
	content string
	called  int
	dir     string
}

func (m *mockKeyGetter) getNewLocalName() string {
	f, _ := ioutil.TempFile(m.dir, "")
	f.Close()
	ioutil.WriteFile(f.Name(), []byte(m.content), 777)
	return f.Name()
}

const mockFetched = "mock fetched"

func (m *mockKeyGetter) get(bucketName string, keyNames []string) []getResult {
	out := make([]getResult, 0, len(keyNames))

	for _, keyName := range keyNames {
		localPath := m.getNewLocalName()
		result := getResult{localPath: &localPath, keyName: keyName,
			bucketName: bucketName, status: mockFetched}
		m.called += 1
		out = append(out, result)
	}
	return out
}

func newMockKeyGetter(content string) *mockKeyGetter {
	tempDir, _ := ioutil.TempDir("", "test_mock_key_getter")
	return &mockKeyGetter{content, 0, tempDir}
}

func TestDiskCachedKeyGetter(t *testing.T) {
	sampleContent := "sample content"
	base := newMockKeyGetter(sampleContent)
	defer os.RemoveAll(base.dir)
	cacheDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)
	var ckg CachedKeyGetter = &diskCachedKeyGetter{base: base, cacheDir: cacheDir}
	keyNames := []string{"key1", "key2"}
	if base.called != 0 {
		t.Logf("Expected 0 calls, but had %v", base.called)
		t.Fail()
	}

	firstResults := ckg.get("bucket", keyNames)
	if len(firstResults) != len(keyNames) {
		t.Fatalf("Expected %v results, but found %v", len(keyNames), len(firstResults))
	}
	if base.called != len(keyNames) {
		t.Logf("Expected %v calls, but had %v", len(keyNames), base.called)
		t.Fail()
	}

	checkResultsReturningPaths := func(results []getResult) map[string]string {
		paths := make(map[string]string)
		for _, result := range results {
			if result.localPath == nil {
				t.Logf("Didn't have a path for the first result for %v", result.keyName)
				t.Fail()
				continue
			}
			if !ckg.has(result.bucketName, result.keyName) {
				t.Logf("Path %v doesn't seem to still be in the cache", *result.localPath)
				t.Fail()
			}
			paths[result.keyName] = *result.localPath

			compareContents(sampleContent, *result.localPath, t)

			if !strings.Contains(*result.localPath, cacheDir) {
				t.Logf("Expected results %v to be downloaded to cache dir %v", *result.localPath, cacheDir)
				t.Fail()
			}

		}
		return paths
	}

	firstPaths := checkResultsReturningPaths(firstResults)
	for _, result := range firstResults {
		if !strings.Contains(result.status, mockFetched) {
			t.Logf("Expected a mock fetch on the first get, but got %v", result.status)
			t.Fail()
		}
	}
	secondResults := ckg.get("bucket", keyNames)

	if base.called != len(keyNames) {
		t.Logf("Expected the number of calls to stay at %v, but had %v", len(keyNames), base.called)
		t.Fail()
	}
	secondPaths := checkResultsReturningPaths(secondResults)

	for _, result := range secondResults {
		if !strings.Contains(result.status, "disk cache hit") {
			t.Logf("Expected a disk cache hit on the first get, but got %v", result.status)
			t.Fail()
		}
	}

	for k, v := range firstPaths {
		if secondPaths[k] != v {
			t.Logf("Got %v for %v on the first get, but %v on the second", v, k, secondPaths[k])
			t.Fail()
		}
	}

}

var rawRequest []byte = []byte(`{"bucket_name":"bucket",
                    "keynames":["key1","key2"],
                    "mutable_bucket": true}`)

func TestUnmarshalling(t *testing.T) {

	var req CacheRequest
	json.Unmarshal(rawRequest, &req)
	if req.BucketName != "bucket" {
		t.Logf("Expected %v for bucketName but got %v", "bucket", req.BucketName)
		t.Fail()
	}
	if len(req.KeyNames) != 2 {
		t.Logf("Expected to have 2 keys, but had %v", len(req.KeyNames))
		t.Fail()
	}
	for i, v := range []string{"key1", "key2"} {
		actual := req.KeyNames[i]
		if actual != v {
			t.Logf("Expected to have %v at position %v but had %v", v, i, actual)
			t.Fail()
		}
	}
	if !req.MutableBucket {
		t.Logf("Should have parsed as a mutable bucket!")
		t.Fail()
	}
}

type ShouldEvictFunc func(getResult) (bool, error)

func (s ShouldEvictFunc) ShouldEvict(r getResult) (bool, error) {
	return s(r)
}

func TestEvictingMutableKeyGetter(t *testing.T) {
	base := newMockKeyGetter("sample content")
	defer os.RemoveAll(base.dir)
	tempDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	dbkg := diskCachedKeyGetter{base, tempDir}
	var evicter ShouldEvicter = ShouldEvictFunc(func(r getResult) (bool, error) {
		return true, nil
	})
	emkg := EvictingMutableKeyGetter{&dbkg, evicter}
	results := emkg.Get("bucket", []string{"key1"}, false)
	if base.called != 1 {
		t.Logf("results log %v", results)
		t.Fatalf("Expected only one call to the base getter after the first call, but had %v", base.called)
	}
	_ = emkg.Get("bucket", []string{"key1"}, false)
	if base.called != 1 {
		t.Fatalf("Expected only one call to the base getter after the second call, but had %v", base.called)
	}
	_ = emkg.Get("bucket", []string{"key1"}, true)
	if base.called != 2 {
		t.Fatalf("Expected a second call to the base getter after a mutable call, but had %v", base.called)
	}

}

type ignoringMutableKeyGetter struct {
	KeyGetter
}

func (i ignoringMutableKeyGetter) Get(bucketName string, keyNames []string, mutable bool) []getResult {
	return i.get(bucketName, keyNames)
}

func TestKeyServer(t *testing.T) {
	t.Skip()
	base := newMockKeyGetter("sample content")
	defer os.RemoveAll(base.dir)

	ks := keyServer{ignoringMutableKeyGetter{base}}
	ts := httptest.NewServer(&ks)
	defer ts.Close()

	http.Post(ts.URL, "application/json", bytes.NewReader(rawRequest))
}
