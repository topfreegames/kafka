package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"
  "github.com/uber-go/zap"
)

// OffsetManager is the main interface consumergroup requires to manage offsets of the consumergroup.
type OffsetManager interface {

	// InitializePartition is called when the consumergroup is starting to consume a
	// partition. It should return the last processed offset for this partition. Note:
	// the same partition can be initialized multiple times during a single run of a
	// consumer group due to other consumer instances coming online and offline.
	InitializePartition(topic string, partition int32) (int64, error)

	// Get next offset.
	GetNextOffset(topic string, partition int32) (int64, error)

	// Force offsetManager to commit all messages already stored.
	CommitOffsets(logger zap.Logger) error

	// MarkAsProcessed tells the offset manager than a certain message has been successfully
	// processed by the consumer, and should be committed. The implementation does not have
	// to store this offset right away, but should return true if it intends to do this at
	// some point.
	//
	// Offsets should generally be increasing if the consumer
	// processes events serially, but this cannot be guaranteed if the consumer does any
	// asynchronous processing. This can be handled in various ways, e.g. by only accepting
	// offsets that are higehr than the offsets seen before for the same partition.
	MarkAsProcessed(topic string, partition int32, offset int64) bool

	// FinalizePartition is called when the consumergroup is done consuming a
	// partition. In this method, the offset manager can flush any remaining offsets to its
	// backend store. It should return an error if it was not able to commit the offset.
	// Note: it's possible that the consumergroup instance will start to consume the same
	// partition again after this function is called.
	FinalizePartition(topic string, partition int32, lastOffset int64, timeout time.Duration, replicaId int, logger zap.Logger) error

	// Close is called when the consumergroup is shutting down. In normal circumstances, all
	// offsets are committed because FinalizePartition is called for all the running partition
	// consumers. You may want to check for this to be true, and try to commit any outstanding
	// offsets. If this doesn't succeed, it should return an error.
	Close(logger zap.Logger) error
}

var (
  UncleanClose = errors.New("KAFKA: Not all offsets were committed before shutdown was completed")
)

// OffsetManagerConfig holds configuration setting son how the offset manager should behave.
type OffsetManagerConfig struct {
	CommitInterval time.Duration // Interval between offset flushes to the backend store.
	VerboseLogging bool          // Whether to enable verbose logging.
	EnableAutoCommit bool        // Whether to enable auto commit.
}

// NewOffsetManagerConfig returns a new OffsetManagerConfig with sane defaults.
func NewOffsetManagerConfig() *OffsetManagerConfig {
	return &OffsetManagerConfig{
		CommitInterval: 10 * time.Second,
	}
}

type (
	topicOffsets    map[int32]*partitionOffsetTracker
	offsetsMap      map[string]topicOffsets
	offsetCommitter func(int64) error
)

type partitionOffsetTracker struct {
	l                      sync.Mutex
	waitingForOffset       int64
	highestProcessedOffset int64
	lastCommittedOffset    int64
	done                   chan struct{}
}

type zookeeperOffsetManager struct {
	config  *OffsetManagerConfig
	l       sync.RWMutex
	offsets offsetsMap
	cg      *ConsumerGroup

	closing, closed chan struct{}
}

// NewZookeeperOffsetManager returns an offset manager that uses Zookeeper
// to store offsets.
func NewZookeeperOffsetManager(cg *ConsumerGroup, config *OffsetManagerConfig, logger zap.Logger) OffsetManager {
	if config == nil {
		config = NewOffsetManagerConfig()
	}

	zom := &zookeeperOffsetManager{
		config:  config,
		cg:      cg,
		offsets: make(offsetsMap),
		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}

	if config.EnableAutoCommit {
		go zom.offsetCommitter(logger)
	}

	return zom
}

func (zom *zookeeperOffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	zom.l.Lock()
	defer zom.l.Unlock()

	if zom.offsets[topic] == nil {
		zom.offsets[topic] = make(topicOffsets)
	}

	nextOffset, err := zom.cg.group.FetchOffset(topic, partition)
	if err != nil {
		return 0, err
	}

	zom.offsets[topic][partition] = &partitionOffsetTracker{
		highestProcessedOffset: nextOffset - 1,
		lastCommittedOffset:    nextOffset - 1,
		waitingForOffset: -1,
		done:                   make(chan struct{}),
	}

	return nextOffset, nil
}

func (zom *zookeeperOffsetManager) FinalizePartition(topic string, partition int32, lastOffset int64, timeout time.Duration, replicaId int, logger zap.Logger) error {
	zom.l.RLock()
	tracker := zom.offsets[topic][partition]
	zom.l.RUnlock()

	if lastOffset >= 0 {
		if lastOffset-tracker.highestProcessedOffset > 0 {
      logger.Info("ZOOKEEPER: Finalizing partition. Waiting before processing remaining messages",
        zap.Int("replicaId", replicaId),
        zap.String("topic", topic),
        zap.Int64("partition", int64(partition)),
        zap.Int64("lastProcessedOffset", tracker.highestProcessedOffset),
        zap.Duration("waitingTimeToProcessMoreMessages", timeout/time.Second),
        zap.Int64("numMessagesToProcess", lastOffset-tracker.highestProcessedOffset),
      )
			if !tracker.waitForOffset(lastOffset, timeout) {
				return fmt.Errorf("REP %d - TIMEOUT waiting for offset %d. Last committed offset: %d", replicaId, lastOffset, tracker.lastCommittedOffset)
			}
		}

		if err := zom.commitOffset(topic, partition, tracker, logger); err != nil {
			return fmt.Errorf("REP %d - FAILED to commit offset %d to Zookeeper. Last committed offset: %d",replicaId, tracker.highestProcessedOffset, tracker.lastCommittedOffset)
		}
	}

	zom.l.Lock()
	delete(zom.offsets[topic], partition)
	zom.l.Unlock()

	return nil
}

func (zom *zookeeperOffsetManager) GetNextOffset(topic string, partition int32) (int64, error) {

	return zom.cg.group.FetchOffset(topic, partition)
}

func (zom *zookeeperOffsetManager) CommitOffsets(logger zap.Logger) error {
	return zom.commitOffsets(logger)
}

func (zom *zookeeperOffsetManager) MarkAsProcessed(topic string, partition int32, offset int64) bool {
	zom.l.RLock()
	defer zom.l.RUnlock()
	if p, ok := zom.offsets[topic][partition]; ok {
		return p.markAsProcessed(offset)
	} else {
		return false
	}
}

func (zom *zookeeperOffsetManager) Close(logger zap.Logger) error {
	if zom.config.EnableAutoCommit {
		close(zom.closing)
		<-zom.closed
	}

	zom.l.Lock()
	defer zom.l.Unlock()

	var closeError error
	for _, partitionOffsets := range zom.offsets {
		if len(partitionOffsets) > 0 {
			closeError = UncleanClose
		}
	}

	return closeError
}

func (zom *zookeeperOffsetManager) offsetCommitter(logger zap.Logger) {
	commitTicker := time.NewTicker(zom.config.CommitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-zom.closing:
			close(zom.closed)
			return
		case <-commitTicker.C:
			zom.commitOffsets(logger)
		}
	}
}

func (zom *zookeeperOffsetManager) commitOffsets(logger zap.Logger) error {
	zom.l.RLock()
	defer zom.l.RUnlock()

	var returnErr error
	for topic, partitionOffsets := range zom.offsets {
		for partition, offsetTracker := range partitionOffsets {
			err := zom.commitOffset(topic, partition, offsetTracker, logger)
			switch err {
			case nil:
				// noop
			default:
				returnErr = err
			}
		}
	}
	return returnErr
}

func (zom *zookeeperOffsetManager) commitOffset(topic string, partition int32, tracker *partitionOffsetTracker, logger zap.Logger) error {
	err := tracker.commit(func(offset int64) error {
		if offset >= 0 {
			return zom.cg.group.CommitOffset(topic, partition, offset+1)
		} else {
			return nil
		}
	})

	if err != nil {
    logger.Warn("ZOOKEEPER: FAILED to commit offset",
      zap.Int64("highestProcessedOffset", tracker.highestProcessedOffset),
      zap.String("topic", topic),
      zap.Int64("partition", int64(partition)),
    )
	} else if zom.config.VerboseLogging {
    logger.Debug("ZOOKEEPER: Committed offset",
      zap.Int64("lastCommittedOffset", tracker.lastCommittedOffset),
      zap.String("topic", topic),
      zap.Int64("partition", int64(partition)),
    )
	}

	return err
}

// MarkAsProcessed marks the provided offset as highest processed offset if
// it's higehr than any previous offset it has received.
func (pot *partitionOffsetTracker) markAsProcessed(offset int64) bool {
	pot.l.Lock()
	defer pot.l.Unlock()
	if offset > pot.highestProcessedOffset {
		pot.highestProcessedOffset = offset
		if pot.waitingForOffset == pot.highestProcessedOffset {
			pot.done <- struct{}{}
		}
		return true
	} else {
		return false
	}
}

// Commit calls a committer function if the highest processed offset is out
// of sync with the last committed offset.
func (pot *partitionOffsetTracker) commit(committer offsetCommitter) error {
	pot.l.Lock()
	defer pot.l.Unlock()

	if pot.highestProcessedOffset > pot.lastCommittedOffset {
		if err := committer(pot.highestProcessedOffset); err != nil {
			return err
		}
		pot.lastCommittedOffset = pot.highestProcessedOffset
		return nil
	} else {
		return nil
	}
}

func (pot *partitionOffsetTracker) waitForOffset(offset int64, timeout time.Duration) bool {
	pot.l.Lock()
	if offset > pot.highestProcessedOffset {
		pot.waitingForOffset = offset
		pot.l.Unlock()
		select {
		case <-pot.done:
			return true
		case <-time.After(timeout):
			return false
		}
	} else {
		pot.l.Unlock()
		return true
	}
}
