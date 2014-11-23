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

type mockKeyGetter struct{ os.File }

func (m *mockKeyGetter) get(bucketName string, keyNames []string) []getResult {
	out := make([]getResult, 0, len(keyNames))
	name := m.Name()

	for _, keyName := range keyNames {
		result := getResult{localPath: &name, keyName: keyName,
			bucketName: bucketName}
		out = append(out, result)
	}
	return out
}

func newMockKeyGetter(content string) (*mockKeyGetter, error) {
	f, err := ioutil.TempFile(os.TempDir(), "")
	f.Write([]byte(content))
	return &mockKeyGetter{*f}, err
}

func TestdiskCachedKeyGetter(t *testing.T) {
	sampleContent := "sample content"
	base, err := newMockKeyGetter(sampleContent)
	defer os.Remove(base.Name())
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := "/tmp/cachetest"
	var ckg CachedKeyGetter = &diskCachedKeyGetter{base: base, cacheDir: cacheDir}
	keyNames := []string{"key1", "key2"}
	firstResults := ckg.get("bucket", keyNames)
	if len(firstResults) != len(keyNames) {
		t.Fatalf("Expected %v results, but only found %v", len(keyNames), len(firstResults))
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
			if !strings.Contains(result.status, "cache miss") {
				t.Logf("Expected a cache miss on the first get, but got %v", result.status)
				t.Fail()
			}

			compareContents(sampleContent, *result.localPath, t)

			if !strings.Contains(*result.localPath, cacheDir) {
				t.Logf("Expected results %v to be downloaded to cache dir %v", *result.localPath, cacheDir)
				t.Fail()
			}
		}
		return paths
	}

	firstPaths := checkResultsReturningPaths(firstResults)
	secondResults := ckg.get("bucket", keyNames)
	secondPaths := checkResultsReturningPaths(secondResults)
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

func TestKeyServer(t *testing.T) {
	base, err := newMockKeyGetter("sample content")
	defer os.Remove(base.Name())
	if err != nil {
		t.Fatal(err)
	}

	ks := keyServer{base}
	ts := httptest.NewServer(&ks)
	defer ts.Close()

	http.Post(ts.URL, "application/json", bytes.NewReader(rawRequest))

}
