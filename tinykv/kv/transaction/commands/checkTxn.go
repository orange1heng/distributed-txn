package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type CheckTxnStatus struct {
	CommandBase
	request *kvrpcpb.CheckTxnStatusRequest
}

func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.LockTs,
		},
		request: request,
	}
}

func (c *CheckTxnStatus) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	key := c.request.PrimaryKey
	response := new(kvrpcpb.CheckTxnStatusResponse)

	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts == txn.StartTS {
		if physical(lock.Ts)+lock.Ttl < physical(c.request.CurrentTs) {
			// 锁过期了
			// YOUR CODE HERE (lab1).
			// Lock has expired, try to rollback it. `mvcc.WriteKindRollback` could be used to
			// represent the type. Try using the interfaces provided by `mvcc.MvccTxn`.
			log.Info("checkTxnStatus rollback the primary lock as it's expired",
				zap.Uint64("lock.TS", lock.Ts),
				zap.Uint64("physical(lock.TS)", physical(lock.Ts)),
				zap.Uint64("txn.StartTS", txn.StartTS),
				zap.Uint64("currentTS", c.request.CurrentTs),
				zap.Uint64("physical(currentTS)", physical(c.request.CurrentTs)))
			// 删除值
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(key)
			}
			// 写入回滚记录，并删除锁
			write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, txn.StartTS, &write)
			txn.DeleteLock(key)
			response.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			// Lock has not expired, leave it alone.
			response.Action = kvrpcpb.Action_NoAction
			response.LockTtl = lock.Ttl
		}

		return response, nil
	}
	// 锁为空或者锁不属于当前事务，即不存在当前事务的锁
	existingWrite, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}
	// 1.存在当前事务的提交记录
	if existingWrite != nil && existingWrite.Kind != mvcc.WriteKindRollback && existingWrite.StartTS == txn.StartTS {
		// The key has already been committed.
		response.CommitVersion = commitTs
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}
	// 2.存在当前事务的回滚记录
	if existingWrite != nil && existingWrite.Kind == mvcc.WriteKindRollback && existingWrite.StartTS == txn.StartTS {
		// The key has already been rolled back, so nothing to do.
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}
	// 3.不存在当前事务的提交/回滚记录
	// YOUR CODE HERE (lab1).
	// The lock never existed, it's still needed to put a rollback record on it so that
	// the stale transaction commands such as prewrite on the key will fail.
	// Note try to set correct `response.Action`,
	// the action types could be found in kvrpcpb.Action_xxx.
	// 锁从未存在过，但仍然需要在其上放置回滚记录，以便过时的事务命令(如预写键)失败。
	// 为什么锁会从来没有存在过？预写就没有成功
	txn.DeleteValue(key)
	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, txn.StartTS, &write)
	response.Action = kvrpcpb.Action_LockNotExistRollback
	return response, nil

}

func physical(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

func (c *CheckTxnStatus) WillWrite() [][]byte {
	return [][]byte{c.request.PrimaryKey}
}
