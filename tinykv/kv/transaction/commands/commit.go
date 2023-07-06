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

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	// YOUR CODE HERE (lab1).
	// Check if the commitTs is invalid, the commitTs must be greater than the transaction startTs. If not
	// report unexpected error.

	if c.request.StartVersion > c.request.CommitVersion {
		return nil, fmt.Errorf("the commitTs must be greater than the transaction startTs")
	}
	response := new(kvrpcpb.CommitResponse)
	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))
	// If there is no correspond lock for this transaction.
	if lock == nil || lock.Ts != txn.StartTS {
		// YOUR CODE HERE (lab1).
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error. Also the commit request could be stale that it's already committed or rolled back.
		// 如果没有锁记录或者被另外一个事务锁住
		// 需要检查此键的提交/回滚记录，如果没有发现，则报告lock not found error。
		// 此外，提交请求可能是过时的，因为它已经提交或回滚。
		currentWrite, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// 存在提交记录，当做重复提交
		if currentWrite != nil && currentWrite.Kind != mvcc.WriteKindRollback && currentWrite.StartTS == txn.StartTS {
			return nil, nil
		}
		// 存在回滚记录
		if currentWrite != nil && currentWrite.Kind == mvcc.WriteKindRollback && currentWrite.StartTS == txn.StartTS {
			respValue := reflect.ValueOf(response)
			keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("The transaction with stast_ts %v has been rolled back ", txn.StartTS)}
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
			return response, nil
		}
		// 不存在当前事务的提交/回滚记录，报告lock not found error。
		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}

	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
