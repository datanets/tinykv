package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	opts   badger.Options
	engine *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	storage := &StandAloneStorage{
		opts: opts,
	}
	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db, err := badger.Open(s.opts)
	if err != nil {
		return err
	}
	s.engine = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandaloneStorageReader{
		engine: s.engine,
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		var err error
		switch data := modify.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.engine, data.Cf, data.Key, data.Value)
		case storage.Delete:
			err = engine_util.DeleteCF(s.engine, data.Cf, data.Key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type StandaloneStorageReader struct {
	engine *badger.DB
	txn    *badger.Txn
}

func (r *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(r.engine, cf, key)
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return val, err
}
func (r *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	r.txn = r.engine.NewTransaction(false)
	return engine_util.NewCFIterator(cf, r.txn)
}
func (r *StandaloneStorageReader) Close() {
	r.txn.Discard()
}
