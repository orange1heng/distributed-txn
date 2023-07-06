package commands

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Rollback struct {
	CommandBase
	request *kvrpcpb.BatchRollbackRequest
}

func NewRollback(request *kvrpcpb.BatchRollbackRequest) Rollback {
	return Rollback{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (r *Rollback) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.BatchRollbackResponse)

	for _, k := range r.request.Keys {
		resp, err := rollbackKey(k, txn, response)
		if resp != nil || err != nil {
			return resp, err
		}
	}
	return response, nil
}

func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	log.Info("rollbackKey",
		zap.Uint64("startTS", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	if lock == nil || lock.Ts != txn.StartTS {
		// There is no lock, check the write status.
		existingWrite, ts, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// Try to insert a rollback record if there's no correspond records, use `mvcc.WriteKindRollback` to represent
		// the type. Also the command could be stale that the record is already rolled back or committed.
		// If there is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
		// if the key has already been rolled back, so nothing to do.
		// If the key has already been committed. This should not happen since the client should never send both
		// commit and rollback requests.
		// There is no write either, presumably the prewrite was lost. We insert a rollback write anyway.

		//if existingWrite == nil {
		//	// YOUR CODE HERE (lab1).
		//	// prewrite丢失，也需要插入一个回滚记录
		//	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		//	txn.PutWrite(key, txn.StartTS, &write)
		//	txn.DeleteLock(key)
		//	return nil, nil
		//} else {
		//	if existingWrite.Kind == mvcc.WriteKindRollback {
		//		// The key has already been rolled back, so nothing to do.
		//		return nil, nil
		//	}
		//
		//	// The key has already been committed. This should not happen since the client should never send both
		//	// commit and rollback requests.
		//	// 错误情况：key已经被提交
		//	err := new(kvrpcpb.KeyError)
		//	err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
		//	respValue := reflect.ValueOf(response)
		//	reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
		//	return response, nil
		//}

		// 1.存在当前事务的提交记录
		// 错误情况：不应该发生
		if existingWrite != nil && existingWrite.Kind != mvcc.WriteKindRollback && existingWrite.StartTS == txn.StartTS {
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			respValue := reflect.ValueOf(response)
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
			return response, nil
		}
		// 2.存在当前事务的回滚记录
		if existingWrite != nil && existingWrite.Kind == mvcc.WriteKindRollback && existingWrite.StartTS == txn.StartTS {
			return nil, nil
		}
		// 3.不存在当前事务的提交/回滚记录
		// prewrite丢失，也需要插入一个回滚记录
		write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, txn.StartTS, &write)
		return response, nil
	}
	// 检查 key 是否被其他事务锁住
	if lock.Ts > txn.StartTS {
		err := new(kvrpcpb.KeyError)
		err.Abort = fmt.Sprintf("key {%v} has already been locked by other transaction with ts %d", key, txn.StartTS)
		respValue := reflect.ValueOf(response)
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
		return response, nil
	}

	if lock.Ts == txn.StartTS {
		// 删除值
		if lock.Kind == mvcc.WriteKindPut {
			txn.DeleteValue(key)
		}
		// 写入回滚记录，并删除锁
		write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, txn.StartTS, &write)
		txn.DeleteLock(key)
	}

	return nil, nil
}

func (r *Rollback) WillWrite() [][]byte {
	return r.request.Keys
}
