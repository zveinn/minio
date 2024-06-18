// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/bytebufferpool"
)

type diskFifoQueueItem struct {
	key  string
	data interface{}
}

// diskFifo - Filestore for persisting items.
type diskFifo[_ any] struct {
	sync.RWMutex
	entryLimit int
	directory  string
	fileExt    string

	entries chan string
}

// NewSRMWStore - Creates an instance for DiskFIFO.
func NewSRMWStore[I any](directory string, limit int, ext string) *diskFifo[I] {
	if limit == 0 {
		limit = defaultLimit
	}

	if ext == "" {
		ext = defaultExt
	}

	return &diskFifo[I]{
		directory:  directory,
		entryLimit: limit,
		fileExt:    ext,
	}
}

// Open - Creates the directory if not present.
func (store *diskFifo[_]) Open() (err error) {
	store.Lock()
	defer store.Unlock()

	err = os.MkdirAll(store.directory, os.FileMode(0o770))
	if err != nil {
		return
	}

	files, err := store.list()
	if err != nil {
		return err
	}

	if len(files) > store.entryLimit {
		store.entries = make(chan string, len(files))
	} else {
		store.entries = make(chan string, store.entryLimit)
	}

	for _, file := range files {
		select {
		case store.entries <- strings.TrimSuffix(file.Name(), store.fileExt):
		default:
		}
	}

	return nil
}

// Extension will return the file extension used
// for the files written to disk.
func (store *diskFifo[_]) Extension() string {
	return store.fileExt
}

// Len returns the queue lenth
func (store *diskFifo[_]) Len() int {
	return len(store.entries)
}

// Truncate - Remove the storage directory from disk.
func (store *diskFifo[_]) Truncate() error {
	return os.Remove(store.directory)
}

// PutBatch - puts multiple items on disk under a single key.
func (store *diskFifo[I]) PutBatch(items []I) error {
	return store.writeBatch(items)
}

// Put - puts an single item on disk.
func (store *diskFifo[I]) Put(item I) error {
	return store.write(item)
}

// ReturnKey - returns a key to the queue.
// if the queue is full then we remove the
// item from disk
func (store *diskFifo[I]) ReturnKey(key string) {
	select {
	case store.entries <- key:
	default:
		_ = os.Remove(filepath.Join(store.directory, key+store.fileExt))
	}
	return
}

// GetKey - pulls a key from the queue.
func (store *diskFifo[I]) GetKey() (key string) {
	select {
	case key = <-store.entries:
	default:
	}
	return
}

// Get - gets a deserialized item related to the given key.
func (store *diskFifo[I]) Get(key string) (*I, error) {
	return store.get(key)
}

// GetRaw - gets the serialized bytes from a fila on disk based on the given key.
func (store *diskFifo[I]) GetRaw(key string) ([]byte, error) {
	return store.getRaw(key)
}

// Delete - deletes and file on disk related to the given key
func (store *diskFifo[I]) Delete(key string) {
	_ = os.Remove(filepath.Join(store.directory, key+store.fileExt))
}

func (store *diskFifo[I]) writeBatch(items []I) error {
	store.Lock()
	defer store.Unlock()
	if len(store.entries) >= store.entryLimit {
		return errLimitExceeded
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	enc := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(buf)

	for i := range items {
		err := enc.Encode(items[i])
		if err != nil {
			return err
		}
	}

	key := strconv.FormatInt(time.Now().UnixNano(), 10)
	path := filepath.Join(store.directory, key+store.fileExt)
	err := os.WriteFile(path, buf.Bytes(), os.FileMode(0o770))
	buf.Reset()
	if err != nil {
		return err
	}

	select {
	case store.entries <- key:
	default:
		_ = os.Remove(path)
		return errLimitExceeded
	}

	return nil
}

func (store *diskFifo[I]) write(item I) error {
	store.Lock()
	defer store.Unlock()
	if len(store.entries) >= store.entryLimit {
		return errLimitExceeded
	}

	eventData, err := json.Marshal(item)
	if err != nil {
		return err
	}

	key := strconv.FormatInt(time.Now().UnixNano(), 10)
	path := filepath.Join(store.directory, key+store.fileExt)
	err = os.WriteFile(path, eventData, os.FileMode(0o770))
	if err != nil {
		return err
	}

	select {
	case store.entries <- key:
	default:
		_ = os.Remove(path)
		return errLimitExceeded
	}

	return nil
}

// GetRaw - gets an item from the store.
func (store *diskFifo[I]) getRaw(key string) (raw []byte, err error) {
	raw, err = os.ReadFile(filepath.Join(store.directory, key+store.fileExt))
	if err != nil {
		return
	}

	if len(raw) == 0 {
		return nil, os.ErrNotExist
	}

	return
}

// Get - gets an item from the store.
func (store *diskFifo[I]) get(key string) (item *I, err error) {
	var eventData []byte
	eventData, err = os.ReadFile(filepath.Join(store.directory, key+store.fileExt))
	if err != nil {
		return
	}

	if len(eventData) == 0 {
		return nil, os.ErrNotExist
	}

	err = json.Unmarshal(eventData, &item)
	if err != nil {
		return
	}

	return
}

// list will read all entries from disk.
// Entries are returned sorted by modtime, oldest first.
// Underlying entry list in store is *not* updated.
func (store *diskFifo[_]) list() ([]os.DirEntry, error) {
	files, err := os.ReadDir(store.directory)
	if err != nil {
		return nil, err
	}

	// Sort the entries.
	for i := range files {
		if files[i].IsDir() {
			slices.Delete(files, i, i+1)
		}
	}

	return files, nil
}
