// Copyright 2023 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

//go:build (arm64 || amd64) && !openbsd

// Package pebble implements the key-value database layer based on pebble.
package pebble

import (
	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

// FaultyDatabase is a persistent key-value store based on the pebble storage engine.
// Apart from basic data storage functionality it also supports batch writes and
// iterating over the keyspace in binary-alphabetical order.
type FaultyDatabase struct {
	fn string     // filename for reporting
	db *pebble.DB // Underlying pebble storage engine

	compTimeMeter       metrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter       metrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter      metrics.Meter // Meter for measuring the data written during compaction
	writeDelayNMeter    metrics.Meter // Meter for measuring the write delay number due to database compaction
	writeDelayMeter     metrics.Meter // Meter for measuring the write delay duration due to database compaction
	diskSizeGauge       metrics.Gauge // Gauge for tracking the size of all the levels in the database
	diskReadMeter       metrics.Meter // Meter for measuring the effective amount of data read
	diskWriteMeter      metrics.Meter // Meter for measuring the effective amount of data written
	memCompGauge        metrics.Gauge // Gauge for tracking the number of memory compaction
	level0CompGauge     metrics.Gauge // Gauge for tracking the number of table compaction in level0
	nonlevel0CompGauge  metrics.Gauge // Gauge for tracking the number of table compaction in non0 level
	seekCompGauge       metrics.Gauge // Gauge for tracking the number of table compaction caused by read opt
	manualMemAllocGauge metrics.Gauge // Gauge for tracking amount of non-managed memory currently allocated

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path

	activeComp          int           // Current number of active compactions
	compStartTime       time.Time     // The start time of the earliest currently-active compaction
	compTime            atomic.Int64  // Total time spent in compaction in ns
	level0Comp          atomic.Uint32 // Total number of level-zero compactions
	nonLevel0Comp       atomic.Uint32 // Total number of non level-zero compactions
	writeDelayStartTime time.Time     // The start time of the latest write stall
	writeDelayCount     atomic.Int64  // Total number of write stall counts
	writeDelayTime      atomic.Int64  // Total time spent in write stalls

	errorInjectionConfig ethdb.ErrorInjectionConfig // Error injection config
}

func (d *FaultyDatabase) SetErrorInjection(config ethdb.ErrorInjectionConfig) {
	d.errorInjectionConfig = config
}

func (d *FaultyDatabase) onCompactionBegin(info pebble.CompactionInfo) {
	if d.activeComp == 0 {
		d.compStartTime = time.Now()
	}
	l0 := info.Input[0]
	if l0.Level == 0 {
		d.level0Comp.Add(1)
	} else {
		d.nonLevel0Comp.Add(1)
	}
	d.activeComp++
}

func (d *FaultyDatabase) onCompactionEnd(info pebble.CompactionInfo) {
	if d.activeComp == 1 {
		d.compTime.Add(int64(time.Since(d.compStartTime)))
	} else if d.activeComp == 0 {
		panic("should not happen")
	}
	d.activeComp--
}

func (d *FaultyDatabase) onWriteStallBegin(b pebble.WriteStallBeginInfo) {
	d.writeDelayStartTime = time.Now()
}

func (d *FaultyDatabase) onWriteStallEnd() {
	d.writeDelayTime.Add(int64(time.Since(d.writeDelayStartTime)))
}

// New returns a wrapped pebble DB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func NewFaulty(file string, cache int, handles int, namespace string, readonly bool) (*FaultyDatabase, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file)
	logger.Info("Allocated cache and file handles", "cache", common.StorageSize(cache*1024*1024), "handles", handles)

	// The max memtable size is limited by the uint32 offsets stored in
	// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
	// Taken from https://github.com/cockroachdb/pebble/blob/master/open.go#L38
	maxMemTableSize := 4<<30 - 1 // Capped by 4 GB

	// Two memory tables is configured which is identical to leveldb,
	// including a frozen memory table and another live one.
	memTableLimit := 2
	memTableSize := cache * 1024 * 1024 / 2 / memTableLimit
	if memTableSize > maxMemTableSize {
		memTableSize = maxMemTableSize
	}
	db := &FaultyDatabase{
		fn:       file,
		log:      logger,
		quitChan: make(chan chan error),
	}
	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles: handles,

		// The size of memory table(as well as the write buffer).
		// Note, there may have more than two memory tables in the system.
		MemTableSize: memTableSize,

		// MemTableStopWritesThreshold places a hard limit on the size
		// of the existent MemTables(including the frozen one).
		// Note, this must be the number of tables not the size of all memtables
		// according to https://github.com/cockroachdb/pebble/blob/master/options.go#L738-L742
		// and to https://github.com/cockroachdb/pebble/blob/master/db.go#L1892-L1903.
		MemTableStopWritesThreshold: memTableLimit,

		// The default compaction concurrency(1 thread),
		// Here use all available CPUs for faster compaction.
		MaxConcurrentCompactions: func() int { return runtime.NumCPU() },

		// Per-level options. Options for at least one level must be specified. The
		// options for the last level are used for all subsequent levels.
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		},
		ReadOnly: readonly,
		EventListener: &pebble.EventListener{
			CompactionBegin: db.onCompactionBegin,
			CompactionEnd:   db.onCompactionEnd,
			WriteStallBegin: db.onWriteStallBegin,
			WriteStallEnd:   db.onWriteStallEnd,
		},
	}
	// Disable seek compaction explicitly. Check https://github.com/ethereum/go-ethereum/pull/20130
	// for more details.
	opt.Experimental.ReadSamplingMultiplier = -1

	// Open the db and recover any potential corruptions
	innerDB, err := pebble.Open(file, opt)
	if err != nil {
		return nil, err
	}
	db.db = innerDB

	db.compTimeMeter = metrics.NewRegisteredMeter(namespace+"compact/time", nil)
	db.compReadMeter = metrics.NewRegisteredMeter(namespace+"compact/input", nil)
	db.compWriteMeter = metrics.NewRegisteredMeter(namespace+"compact/output", nil)
	db.diskSizeGauge = metrics.NewRegisteredGauge(namespace+"disk/size", nil)
	db.diskReadMeter = metrics.NewRegisteredMeter(namespace+"disk/read", nil)
	db.diskWriteMeter = metrics.NewRegisteredMeter(namespace+"disk/write", nil)
	db.writeDelayMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/duration", nil)
	db.writeDelayNMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/counter", nil)
	db.memCompGauge = metrics.NewRegisteredGauge(namespace+"compact/memory", nil)
	db.level0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/level0", nil)
	db.nonlevel0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/nonlevel0", nil)
	db.seekCompGauge = metrics.NewRegisteredGauge(namespace+"compact/seek", nil)
	db.manualMemAllocGauge = metrics.NewRegisteredGauge(namespace+"memory/manualalloc", nil)

	// Start up the metrics gathering and return
	go db.meter(metricsGatheringInterval)
	return db, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (d *FaultyDatabase) Close() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()

	if d.quitChan != nil {
		errc := make(chan error)
		d.quitChan <- errc
		if err := <-errc; err != nil {
			d.log.Error("Metrics collection failed", "err", err)
		}
		d.quitChan = nil
	}
	return d.db.Close()
}

// Has retrieves if a key is present in the key-value store.
func (d *FaultyDatabase) Has(key []byte) (bool, error) {
	_, closer, err := d.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// Get retrieves the given key if it's present in the key-value store.
func (d *FaultyDatabase) Get(key []byte) ([]byte, error) {
	dat, closer, err := d.db.Get(key)
	if err != nil {
		return nil, err
	}
	ret := make([]byte, len(dat))
	copy(ret, dat)
	closer.Close()

	// Inject a failure if enabled
	if d.errorInjectionConfig.EnableInjection {
		d.errorInjectionConfig.InjectCount++

		if d.errorInjectionConfig.InjectCount == d.errorInjectionConfig.FailWhen {
			switch d.errorInjectionConfig.ErrorType {
			case ethdb.ErrorRead:
				return nil, pebble.ErrNotFound
			case ethdb.CorruptedReadAllZero:
				for i := 0; i < len(ret); i++ {
					ret[i] = 0
				}
			case ethdb.CorruptedReadGarbage:
				for i := 0; i < len(ret); i++ {
					ret[i] = byte(rand.Intn(255))
				}
			}
		}
	}

	return ret, nil
}

// Put inserts the given value into the key-value store.
func (d *FaultyDatabase) Put(key []byte, value []byte) error {
	return d.db.Set(key, value, pebble.NoSync)
}

// Delete removes the key from the key-value store.
func (d *FaultyDatabase) Delete(key []byte) error {
	return d.db.Delete(key, nil)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (d *FaultyDatabase) NewBatch() ethdb.Batch {
	return &batch{
		b: d.db.NewBatch(),
	}
}

// NewBatchWithSize creates a write-only database batch with pre-allocated buffer.
// It's not supported by pebble, but pebble has better memory allocation strategy
// which turns out a lot faster than leveldb. It's performant enough to construct
// batch object without any pre-allocated space.
func (d *FaultyDatabase) NewBatchWithSize(_ int) ethdb.Batch {
	return &batch{
		b: d.db.NewBatch(),
	}
}

// NewSnapshot creates a database snapshot based on the current state.
// The created snapshot will not be affected by all following mutations
// happened on the database.
// Note don't forget to release the snapshot once it's used up, otherwise
// the stale data will never be cleaned up by the underlying compactor.
func (d *FaultyDatabase) NewSnapshot() (ethdb.Snapshot, error) {
	snap := d.db.NewSnapshot()
	return &snapshot{db: snap}, nil
}

// Stat returns a particular internal stat of the database.
func (d *FaultyDatabase) Stat(property string) (string, error) {
	return "", nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (d *FaultyDatabase) Compact(start []byte, limit []byte) error {
	// There is no special flag to represent the end of key range
	// in pebble(nil in leveldb). Use an ugly hack to construct a
	// large key to represent it.
	// Note any prefixed database entry will be smaller than this
	// flag, as for trie nodes we need the 32 byte 0xff because
	// there might be a shared prefix starting with a number of
	// 0xff-s, so 32 ensures than only a hash collision could touch it.
	// https://github.com/cockroachdb/pebble/issues/2359#issuecomment-1443995833
	if limit == nil {
		limit = bytes.Repeat([]byte{0xff}, 32)
	}
	return d.db.Compact(start, limit, true) // Parallelization is preferred
}

// Path returns the path to the database directory.
func (d *FaultyDatabase) Path() string {
	return d.fn
}

// meter periodically retrieves internal pebble counters and reports them to
// the metrics subsystem.
func (d *FaultyDatabase) meter(refresh time.Duration) {
	var errc chan error
	timer := time.NewTimer(refresh)
	defer timer.Stop()

	// Create storage and warning log tracer for write delay.
	var (
		compTimes        [2]int64
		writeDelayTimes  [2]int64
		writeDelayCounts [2]int64
		compWrites       [2]int64
		compReads        [2]int64

		nWrites [2]int64
	)

	// Iterate ad infinitum and collect the stats
	for i := 1; errc == nil; i++ {
		var (
			compWrite int64
			compRead  int64
			nWrite    int64

			metrics            = d.db.Metrics()
			compTime           = d.compTime.Load()
			writeDelayCount    = d.writeDelayCount.Load()
			writeDelayTime     = d.writeDelayTime.Load()
			nonLevel0CompCount = int64(d.nonLevel0Comp.Load())
			level0CompCount    = int64(d.level0Comp.Load())
		)
		writeDelayTimes[i%2] = writeDelayTime
		writeDelayCounts[i%2] = writeDelayCount
		compTimes[i%2] = compTime

		for _, levelMetrics := range metrics.Levels {
			nWrite += int64(levelMetrics.BytesCompacted)
			nWrite += int64(levelMetrics.BytesFlushed)
			compWrite += int64(levelMetrics.BytesCompacted)
			compRead += int64(levelMetrics.BytesRead)
		}

		nWrite += int64(metrics.WAL.BytesWritten)

		compWrites[i%2] = compWrite
		compReads[i%2] = compRead
		nWrites[i%2] = nWrite

		if d.writeDelayNMeter != nil {
			d.writeDelayNMeter.Mark(writeDelayCounts[i%2] - writeDelayCounts[(i-1)%2])
		}
		if d.writeDelayMeter != nil {
			d.writeDelayMeter.Mark(writeDelayTimes[i%2] - writeDelayTimes[(i-1)%2])
		}
		if d.compTimeMeter != nil {
			d.compTimeMeter.Mark(compTimes[i%2] - compTimes[(i-1)%2])
		}
		if d.compReadMeter != nil {
			d.compReadMeter.Mark(compReads[i%2] - compReads[(i-1)%2])
		}
		if d.compWriteMeter != nil {
			d.compWriteMeter.Mark(compWrites[i%2] - compWrites[(i-1)%2])
		}
		if d.diskSizeGauge != nil {
			d.diskSizeGauge.Update(int64(metrics.DiskSpaceUsage()))
		}
		if d.diskReadMeter != nil {
			d.diskReadMeter.Mark(0) // pebble doesn't track non-compaction reads
		}
		if d.diskWriteMeter != nil {
			d.diskWriteMeter.Mark(nWrites[i%2] - nWrites[(i-1)%2])
		}
		// See https://github.com/cockroachdb/pebble/pull/1628#pullrequestreview-1026664054
		manuallyAllocated := metrics.BlockCache.Size + int64(metrics.MemTable.Size) + int64(metrics.MemTable.ZombieSize)
		d.manualMemAllocGauge.Update(manuallyAllocated)
		d.memCompGauge.Update(metrics.Flush.Count)
		d.nonlevel0CompGauge.Update(nonLevel0CompCount)
		d.level0CompGauge.Update(level0CompCount)
		d.seekCompGauge.Update(metrics.Compact.ReadCount)

		// Sleep a bit, then repeat the stats collection
		select {
		case errc = <-d.quitChan:
			// Quit requesting, stop hammering the database
		case <-timer.C:
			timer.Reset(refresh)
			// Timeout, gather a new set of stats
		}
	}
	errc <- nil
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (d *FaultyDatabase) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	iter := d.db.NewIter(&pebble.IterOptions{
		LowerBound: append(prefix, start...),
		UpperBound: upperBound(prefix),
	})
	iter.First()
	return &pebbleIterator{iter: iter, moved: true}
}
