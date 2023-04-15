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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

//go:build (arm64 || amd64) && !openbsd

package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
)

// NewPebbleDBDatabase creates a persistent key-value database without a freezer
// moving immutable chain segments into cold storage.
func NewFaultyPebbleDBDatabase(file string, cache int, handles int, namespace string, readonly bool, errorInjectionConfig ethdb.ErrorInjectionConfig) (ethdb.Database, error) {
	db, err := pebble.NewFaulty(file, cache, handles, namespace, readonly)
	db.SetErrorInjection(errorInjectionConfig)
	if err != nil {
		return nil, err
	}
	return NewDatabase(db), nil
}
