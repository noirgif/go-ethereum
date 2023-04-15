// Copyright 2015 The go-ethereum Authors
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

package node

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

// This test checks that open databases are closed with node.
func TestFaultyNodeCloseClosesDB(t *testing.T) {
	stack, _ := NewFaulty(testNodeConfig(), ethdb.ErrorInjectionConfig{})
	defer stack.Close()

	db, err := stack.OpenDatabase("mydb", 0, 0, "", false)
	if err != nil {
		t.Fatal("can't open DB:", err)
	}
	if err = db.Put([]byte{}, []byte{}); err != nil {
		t.Fatal("can't Put on open DB:", err)
	}

	stack.Close()
	if err = db.Put([]byte{}, []byte{}); err == nil {
		t.Fatal("Put succeeded after node is closed")
	}
}

// This test checks that OpenDatabase can be used from within a Lifecycle Start method.
func TestFaultyNodeOpenDatabaseFromLifecycleStart(t *testing.T) {
	stack, _ := NewFaulty(testNodeConfig(), ethdb.ErrorInjectionConfig{})
	defer stack.Close()

	var db ethdb.Database
	var err error
	stack.RegisterLifecycle(&InstrumentedService{
		startHook: func() {
			db, err = stack.OpenDatabase("mydb", 0, 0, "", false)
			if err != nil {
				t.Fatal("can't open DB:", err)
			}
		},
		stopHook: func() {
			db.Close()
		},
	})

	stack.Start()
	stack.Close()
}

// This test checks that OpenDatabase can be used from within a Lifecycle Stop method.
func TestFaultyNodeOpenDatabaseFromLifecycleStop(t *testing.T) {
	stack, _ := NewFaulty(testNodeConfig(), ethdb.ErrorInjectionConfig{})
	defer stack.Close()

	stack.RegisterLifecycle(&InstrumentedService{
		stopHook: func() {
			db, err := stack.OpenDatabase("mydb", 0, 0, "", false)
			if err != nil {
				t.Fatal("can't open DB:", err)
			}
			db.Close()
		},
	})

	stack.Start()
	stack.Close()
}
