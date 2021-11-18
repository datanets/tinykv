package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	var isNotFound bool
	if val == nil {
		isNotFound = true
	}
	// todo yufan get = nil?
	return &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: isNotFound,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modifys := make([]storage.Modify, 1)

	modifys[0] = storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}

	err := server.storage.Write(req.Context, modifys)
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.RawPutResponse{}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modifys := make([]storage.Modify, 1)

	modifys[0] = storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}

	if err := server.storage.Write(req.Context, modifys); err != nil {
		return nil, err
	}

	resp := &kvrpcpb.RawDeleteResponse{}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var (
		cf    = req.Cf
		start = req.StartKey
		limit = req.Limit
	)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	result := make([]*kvrpcpb.KvPair, 0, limit)
	var count uint32
	iter := reader.IterCF(cf)
	iter.Seek(start)
	for iter.Valid() {
		if count == limit {
			break
		}
		key := iter.Item().Key()
		val, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		result = append(result, &kvrpcpb.KvPair{Key: key, Value: val})
		iter.Next()
		count++
	}
	iter.Close()
	reader.Close()
	return &kvrpcpb.RawScanResponse{
		Kvs: result,
	}, nil
}
