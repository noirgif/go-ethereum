package rawdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// openKeyValueDatabase opens a disk-based key-value database, e.g. leveldb or pebble.
//
//	   type == null          type != null
//	+----------------------------------------
//
// db is non-existent |  leveldb default  |  specified type
// db is existent     |  from db          |  specified type (if compatible)
func openFaultyKeyValueDatabase(o OpenOptions, e ethdb.ErrorInjectionConfig) (ethdb.Database, error) {
	existingDb := hasPreexistingDb(o.Directory)
	if len(existingDb) != 0 && len(o.Type) != 0 && o.Type != existingDb {
		return nil, fmt.Errorf("db.engine choice was %v but found pre-existing %v database in specified data directory", o.Type, existingDb)
	}
	if o.Type == dbPebble || existingDb == dbPebble {
		if PebbleEnabled {
			log.Info("Using pebble as the backing database")
			return NewFaultyPebbleDBDatabase(o.Directory, o.Cache, o.Handles, o.Namespace, o.ReadOnly, e)
		} else {
			return nil, errors.New("db.engine 'pebble' not supported on this platform")
		}
	}
	if len(o.Type) != 0 && o.Type != dbLeveldb {
		return nil, fmt.Errorf("unknown db.engine %v", o.Type)
	}
	log.Info("Using leveldb as the backing database")
	// Use leveldb, either as default (no explicit choice), or pre-existing, or chosen explicitly
	return NewLevelDBDatabase(o.Directory, o.Cache, o.Handles, o.Namespace, o.ReadOnly)
}

// Open opens both a disk-based key-value database such as leveldb or pebble, but also
// integrates it with a freezer database -- if the AncientDir option has been
// set on the provided OpenOptions.
// The passed o.AncientDir indicates the path of root ancient directory where
// the chain freezer can be opened.
func OpenFaulty(o OpenOptions, e ethdb.ErrorInjectionConfig) (ethdb.Database, error) {
	kvdb, err := openFaultyKeyValueDatabase(o, e)
	if err != nil {
		return nil, err
	}
	if len(o.AncientsDirectory) == 0 {
		return kvdb, nil
	}
	frdb, err := NewDatabaseWithFreezer(kvdb, o.AncientsDirectory, o.Namespace, o.ReadOnly)
	if err != nil {
		kvdb.Close()
		return nil, err
	}
	return frdb, nil
}
