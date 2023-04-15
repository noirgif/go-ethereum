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

package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
)

func TestFaultyPebbleDB(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			db, err := pebble.Open("", &pebble.Options{
				FS: vfs.NewMem(),
			})
			if err != nil {
				t.Fatal(err)
			}
			return &Database{
				db: db,
			}
		})
	})
}

func BenchmarkFaultyPebbleDB(b *testing.B) {
	dbtest.BenchDatabaseSuite(b, func() ethdb.KeyValueStore {
		db, err := pebble.Open("", &pebble.Options{
			FS: vfs.NewMem(),
		})
		if err != nil {
			b.Fatal(err)
		}
		return &FaultyDatabase{
			db: db,
		}
	})
}

func TestErrorInjection(t *testing.T) {
	db, err := pebble.Open("", &pebble.Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}
	faulty := &FaultyDatabase{
		db: db,
	}
	faulty.SetErrorInjection(ethdb.ErrorInjectionConfig{
		EnableInjection: true,
		ErrorType:       ethdb.ErrorRead,
		InjectCount:     1,
	})

	// Test that the error injection works.
	if err := faulty.Put([]byte("foo"), []byte("bar")); err != nil {
		_, err := faulty.Get([]byte("foo"))
		if err != pebble.ErrNotFound {
			t.Fatal("error injection failed")
		}
	}
}
