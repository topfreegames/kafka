package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
	"github.com/samuel/go-zookeeper/zk"
  "github.com/uber-go/zap"
)

var (
	AlreadyClosing = errors.New("The consumer group is already shutting down.")
)

type Config struct {
	*sarama.Config

	Zookeeper *kazoo.Config

	Offsets struct {
		Initial           int64         // The initial offset method to use if the consumer has no previously stored offset. Must be either sarama.OffsetOldest (default) or sarama.OffsetNewest.
		ProcessingTimeout time.Duration // Time to wait for all the offsets for a partition to be processed after stopping to consume from it. Defaults to 1 minute.
		CommitInterval    time.Duration // The interval between which the prossed offsets are commited.
	}

	EnableOffsetAutoCommit bool // Enable offset auto commit.
}

func NewConfig() *Config {
	config := &Config{}
	config.Config = sarama.NewConfig()
	config.Zookeeper = kazoo.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 60 * time.Second
	config.Offsets.CommitInterval = 10 * time.Second
	config.EnableOffsetAutoCommit = true

	return config
}

func (cgc *Config) Validate() error {
	if cgc.Zookeeper.Timeout <= 0 {
    return sarama.ConfigurationError("SARAMA: ZookeeperTimeout should have a duration > 0")
	}

	if cgc.Offsets.CommitInterval <= 0 {
    return sarama.ConfigurationError("SARAMA: CommitInterval should have a duration > 0")
	}

	if cgc.Offsets.Initial != sarama.OffsetOldest && cgc.Offsets.Initial != sarama.OffsetNewest {
    return errors.New("SARAMA: Offsets.Initial should be sarama.OffsetOldest or sarama.OffsetNewest.")
	}

	if cgc.Config != nil {
		if err := cgc.Config.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// The ConsumerGroup type holds all the information for a consumer that is part
// of a consumer group. Call JoinConsumerGroup to start a consumer.
type ConsumerGroup struct {
	config *Config

	consumer sarama.Consumer
	kazoo    *kazoo.Kazoo
	group    *kazoo.Consumergroup
	instance *kazoo.ConsumergroupInstance

	zookeeper []string
	topics []string

	wg             sync.WaitGroup
	singleShutdown sync.Once

	reloadMutex sync.Mutex
	singleReload sync.Once

	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	stopper  chan struct{}

	consumers kazoo.ConsumergroupInstanceList

	offsetManager OffsetManager

	replicaId int
}

// Connects to a consumer group, using Zookeeper for auto-discovery
func JoinConsumerGroup(name string, topics []string, zookeeper []string, config *Config, replicaId int, logger zap.Logger) (*ConsumerGroup, error) {

	config.ClientID = name
	consumerGroup := &ConsumerGroup {
		config: config,
		zookeeper: zookeeper,
		topics: topics,
		replicaId: replicaId,
	}
	err := consumerGroup.Load(logger)
	return consumerGroup, err
}

func (cg *ConsumerGroup) Load(logger zap.Logger) error {
	var kz *kazoo.Kazoo
	var err error
	if kz, err = kazoo.NewKazoo(cg.zookeeper, cg.config.Zookeeper); err != nil {
		return err
	}

  logger.Info("KAFKA: Getting broker list for replica",
    zap.Int("replicaId", cg.replicaId),
  )
	brokers, err := kz.BrokerList()
	if err != nil {
		kz.Close()
		return err
	}

	group := kz.Consumergroup(cg.config.ClientID)
	instance := group.NewInstance()

	var consumer sarama.Consumer
	if consumer, err = sarama.NewConsumer(brokers, cg.config.Config); err != nil {
		kz.Close()
		return err
	}

	cg.kazoo = kz
	cg.group = group
	cg.instance = instance
	cg.messages = make(chan *sarama.ConsumerMessage, cg.config.ChannelBufferSize)
	cg.consumer = consumer
	cg.singleShutdown = sync.Once{}
	cg.errors = make(chan *sarama.ConsumerError, cg.config.ChannelBufferSize)
	cg.stopper = make(chan struct{})

	if exists, err := cg.group.Exists(); err != nil {
    logger.Fatal("KAFKA: Replica failed to check existence of consumergroup",
      zap.Int("replicaId", cg.replicaId),
      zap.Error(err),
    )
		consumer.Close()
		kz.Close()
		return err
	} else if !exists {
    logger.Info("KAFKA: Consumergroup does not exist, creating it",
      zap.Int("replicaId", cg.replicaId),
      zap.String("consumerGroupName", cg.group.Name),
    )
		if err := cg.group.Create(); err != nil {
      logger.Fatal("KAFKA: Failed to create consumergroup in Zookeeper",
        zap.Int("replicaId", cg.replicaId),
        zap.Error( err),
      )
			consumer.Close()
			kz.Close()
			return err
		}
	}

	if err := cg.instance.Register(cg.topics); err != nil {
    logger.Fatal("KAFKA: Failed to create consumer instance",
      zap.Int("replicaId", cg.replicaId),
      zap.Error(err),
    )
		return err
	} else {
    logger.Info("KAFKA: Consumer instance registered",
      zap.Int("replicaId", cg.replicaId),
    )
	}

	offsetConfig := OffsetManagerConfig {
		CommitInterval: cg.config.Offsets.CommitInterval,
		EnableAutoCommit: cg.config.EnableOffsetAutoCommit,
	}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig, logger)
	go cg.topicListConsumer(cg.topics, logger)

	return nil
}

func (cg *ConsumerGroup) GetNewestOffset(topic string, partition int32, brokers []string) (int64, error) {
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	return client.GetOffset(topic, partition, sarama.OffsetNewest)
}

func (cg *ConsumerGroup) GetSizeOfAvailableMessages() int {
	cg.reloadMutex.Lock()
	defer cg.reloadMutex.Unlock()
	return len(cg.messages)
}

func (cg *ConsumerGroup) GetBatchOfMessages(batchSize int, logger zap.Logger) []string {
	offsets := make(map[string]map[int32]int64)
	groupOfMessages := []string{}
	if batchSize == 0 {
		return groupOfMessages
	}
	counter := 0
	for {
		if counter == batchSize {
			break
		}
		cg.reloadMutex.Lock()
		select {
		case message := <- cg.messages:
			if offsets[message.Topic] == nil {
				offsets[message.Topic] = make(map[int32]int64)
			}
			if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset - 1 {
        logger.Error("KAFKA: Unexpected offset for message topic and partition",
					zap.String("messageTopic", message.Topic),
					zap.Int64("messagePartition", int64(message.Partition)),
					zap.Int64("offsetExpected", offsets[message.Topic][message.Partition] + 1),
					zap.Int64("offsetFound", message.Offset),
					zap.Int64("offsetDifference", message.Offset - offsets[message.Topic][message.Partition] + 1),
				)
				continue
			}
			groupOfMessages = append(groupOfMessages, string(message.Value))
			offsets[message.Topic][message.Partition] = message.Offset
			cg.CommitUpto(message)
			counter += 1
			cg.reloadMutex.Unlock()
		default:
			cg.reloadMutex.Unlock()
			continue
		}
	}
	return groupOfMessages
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Messages() <-chan *sarama.ConsumerMessage {
	return cg.messages
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Errors() <-chan *sarama.ConsumerError {
	return cg.errors
}

func (cg *ConsumerGroup) Closed() bool {
	return cg.instance == nil
}


func (cg *ConsumerGroup) reload(logger zap.Logger) error {
	cg.reloadMutex.Lock()
	defer cg.reloadMutex.Unlock()
	cg.singleReload.Do(func() {
    logger.Info("KAFKA: Closing down old connections for replica",
      zap.Int("replicaId", cg.replicaId),
    )
		err := cg.Close(logger)
		if err != nil {
      logger.Error("KAFKA: Failed to close consumergroup for replica",
        zap.Int("replicaId", cg.replicaId),
        zap.Error(err),
      )
		}
		cg.Load(logger)
	})
	return nil
}

func (cg *ConsumerGroup) Close(logger zap.Logger) error {
	shutdownError := AlreadyClosing
	cg.singleShutdown.Do(func() {
		defer cg.kazoo.Close()

		shutdownError = nil
		close(cg.stopper)
		cg.wg.Wait()
		if err := cg.offsetManager.Close(logger); err != nil {
      logger.Error("KAFKA: FAILED closing the offset manager for replica!",
        zap.Int("replicaId", cg.replicaId),
        zap.Error(err),
      )
		}
		if shutdownError = cg.instance.Deregister(); shutdownError != nil {
      logger.Warn("KAFKA: Replica FAILED deregistering consumer instance",
        zap.Int("replicaId", cg.replicaId),
        zap.Error(shutdownError),
      )
		} else {
      logger.Info("KAFKA: Replica deregistered consumer instance",
        zap.Int("replicaId", cg.replicaId),
        zap.String("instanceId", cg.instance.ID),
      )
		}
		if shutdownError = cg.consumer.Close(); shutdownError != nil {
			logger.Error("Replica FAILED closing the Sarama client",
        zap.Int("replicaId", cg.replicaId),
        zap.Error(shutdownError),
      )
		}
		close(cg.messages)
		close(cg.errors)
		cg.instance = nil
	})
	return shutdownError
}

func (cg *ConsumerGroup) Logf(format string, args ...interface{}) {
	var identifier string
	if cg.instance == nil {
		identifier = "(defunct)"
	} else {
		identifier = cg.instance.ID[len(cg.instance.ID)-12:]
	}
	sarama.Logger.Printf("[%s/%s] %s", cg.group.Name, identifier, fmt.Sprintf(format, args...))
}

func (cg *ConsumerGroup) InstanceRegistered() (bool, error) {
	return cg.instance.Registered()
}

func (cg *ConsumerGroup) CommitOffsets(logger zap.Logger) error {
	cg.reloadMutex.Lock()
	defer cg.reloadMutex.Unlock()
	return cg.offsetManager.CommitOffsets(logger)
}

func (cg *ConsumerGroup) CommitUpto(message *sarama.ConsumerMessage) error {
	cg.offsetManager.MarkAsProcessed(message.Topic, message.Partition, message.Offset)
	return nil
}

func (cg *ConsumerGroup) topicListConsumer(topics []string, logger zap.Logger) {
	for {
		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.group.WatchInstances()
		if err != nil {
      logger.Fatal("KAFKA: FAILED to get list of registered consumer instances for replica",
        zap.Int("replicaId", cg.replicaId),
        zap.Error(err),
      )
			return
		}

		cg.consumers = consumers
    logger.Info("KAFKA: Got currently registered consumers for replica",
      zap.Int("replicaId", cg.replicaId),
      zap.Int("numRegisteredConsumers", len(cg.consumers)),
    )

		stopper := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(1)
			go cg.topicConsumer(topic, cg.messages, cg.errors, stopper, logger)
		}

		select {
		case <-cg.stopper:
			close(stopper)
			return

		case event := <-consumerChanges:
			if event.Err == zk.ErrSessionExpired || event.Err == zk.ErrConnectionClosed {
        logger.Info("KAFKA: Session was expired, reloading consumer for replica",
          zap.Int("replicaId", cg.replicaId),
        )
				go cg.reload(logger)
				<- cg.stopper
				close(stopper)
				return
			} else {
        logger.Info("KAFKA: Triggering rebalance due to consumer list change in replica",
          zap.Int("replicaId", cg.replicaId),
        )
				close(stopper)
				cg.wg.Wait()
			}
		}
	}
}

func (cg *ConsumerGroup) topicConsumer(topic string, messages chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, stopper <-chan struct{}, logger zap.Logger) {
	defer cg.wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

  logger.Info("KAFKA: Replica started consumer for topic",
    zap.Int("replicaId", cg.replicaId),
    zap.String("topic", topic),
  )

	// Fetch a list of partition IDs
	partitions, err := cg.kazoo.Topic(topic).Partitions()
	if err != nil {
    logger.Fatal("KAFKA: Replica FAILED to get list of partitions for topic",
      zap.Int("replicaId", cg.replicaId),
      zap.String("topic", topic),
      zap.Error(err),
    )
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	partitionLeaders, err := retrievePartitionLeaders(partitions)
	if err != nil {
    logger.Fatal("KAFKA: Replica FAILED to get leaders of partitions for topic",
      zap.Int("replicaId", cg.replicaId),
      zap.String("topic", topic),
      zap.Error(err),
    )
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitionLeaders)
	myPartitions := dividedPartitions[cg.instance.ID]
  logger.Info("KAFKA: Replica is claiming partitions",
    zap.Int("replicaId", cg.replicaId),
    zap.String("topic", topic),
    zap.Int("claimedPartitions", len(myPartitions)),
    zap.Int("numPartitionLeaders", len(partitionLeaders)),
  )
	// Consume all the assigned partitions
	var wg sync.WaitGroup
	myPartitionsStr := ""
	for _, pid := range myPartitions {
		myPartitionsStr += fmt.Sprintf("%d ", pid.ID)
		wg.Add(1)
		go cg.partitionConsumer(topic, pid.ID, messages, errors, &wg, stopper, logger)
	}
  logger.Info("KAFKA: Retrieved replica's partitions",
    zap.Int("replicaId", cg.replicaId),
    zap.String("myPartitions", myPartitionsStr),
  )
	wg.Wait()
  logger.Info("KAFKA: Replica stopped consumer of a topic",
    zap.Int("replicaId", cg.replicaId),
    zap.String("topic", topic),
  )
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, messages chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, wg *sync.WaitGroup, stopper <-chan struct{}, logger zap.Logger) {
	defer wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	for maxRetries, tries := 3, 0; tries < maxRetries; tries++ {
		if err := cg.instance.ClaimPartition(topic, partition); err == nil {
			break
		} else if err == kazoo.ErrPartitionClaimedByOther && tries+1 < maxRetries {
			time.Sleep(1 * time.Second)
		} else {
      logger.Warn("KAFKA: Replica FAILED to claim partition",
        zap.Int("replicaId", cg.replicaId),
        zap.String("topic", topic),
        zap.Int64("partition", int64(partition)),
        zap.Error(err),
      )
			return
		}
	}
	defer cg.instance.ReleasePartition(topic, partition)

	nextOffset, err := cg.offsetManager.InitializePartition(topic, partition)

	if err != nil {
    logger.Error("KAFKA: Replica FAILED to determine initial offset",
      zap.Int("replicaId", cg.replicaId),
      zap.String("topic", topic),
      zap.Int64("partition", int64(partition)),
      zap.Error(err),
    )
		return
	}

	if nextOffset >= 0 {
    logger.Info("KAFKA: Replica partition consumer starting at offset",
      zap.Int("replicaId", cg.replicaId),
      zap.String("topic", topic),
      zap.Int64("partition", int64(partition)),
      zap.Int64("nextOffset", nextOffset),
    )
	} else {
		nextOffset = cg.config.Offsets.Initial
		if nextOffset == sarama.OffsetOldest {
      logger.Info("KAFKA: Replica partition consumer starting at the oldest available offset",
        zap.Int("replicaId", cg.replicaId),
        zap.String("topic", topic),
        zap.Int64("partition", int64(partition)),
      )
		} else if nextOffset == sarama.OffsetNewest {
      logger.Info("KAFKA: Replica partition consumer listening for new messages only",
        zap.Int("replicaId", cg.replicaId),
        zap.String("topic", topic),
        zap.Int64("partition", int64(partition)),
      )
		}
	}

	consumer, err := cg.consumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
    logger.Warn("KAFKA: Replica partition consumer offset out of Range",
      zap.Int("replicaId", cg.replicaId),
      zap.String("topic", topic),
      zap.Int64("partition", int64(partition)),
    )
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if cg.config.Offsets.Initial == sarama.OffsetOldest {
			nextOffset = sarama.OffsetOldest
      logger.Info("KAFKA: Replica partition consumer offset reset to oldest available offset",
        zap.Int("replicaId", cg.replicaId),
        zap.String("topic", topic),
        zap.Int64("partition", int64(partition)),
      )
		} else {
			nextOffset = sarama.OffsetNewest
      logger.Info("KAFKA: Replica partition consumer offset reset to newest available offset",
        zap.Int("replicaId", cg.replicaId),
        zap.String("topic", topic),
        zap.Int64("partition", int64(partition)),
      )
		}
		// retry the consumePartition with the adjusted offset
		consumer, err = cg.consumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
    logger.Fatal("KAFKA: Replica FAILED to start partition consumer",
      zap.Int("replicaId", cg.replicaId),
      zap.String("topic", topic),
      zap.Int64("partition", int64(partition)),
      zap.Error(err),
    )
		return
	}
	defer consumer.Close()

	err = nil
	var lastOffset int64 = -1 // aka unknown
partitionConsumerLoop:
	for {
		select {
		case <-stopper:
			break partitionConsumerLoop

		case err := <-consumer.Errors():
			for {
				select {
				case errors <- err:
					continue partitionConsumerLoop

				case <-stopper:
					break partitionConsumerLoop
				}
			}

		case message := <-consumer.Messages():
			for {
				select {
				case <-stopper:
					break partitionConsumerLoop

				case messages <- message:
					lastOffset = message.Offset
					continue partitionConsumerLoop
				}
			}
		}
	}

  logger.Info("KAFKA: Replica is stopping partition consumer at offset",
    zap.Int("replicaId", cg.replicaId),
    zap.String("topic", topic),
    zap.Int64("partition", int64(partition)),
    zap.Int64("lastOffset", lastOffset),
  )
	if err = cg.offsetManager.FinalizePartition(topic, partition, lastOffset, cg.config.Offsets.ProcessingTimeout, cg.replicaId, logger); err != nil {
    logger.Fatal("KAFKA: Replica error trying to stop partition consumer",
      zap.Int("replicaId", cg.replicaId),
      zap.String("topic", topic),
      zap.Int64("partition", int64(partition)),
      zap.Error(err),
    )
	}
  logger.Info("KAFKA: Replica successfully stoped partition",
    zap.Int("replicaId", cg.replicaId),
    zap.String("topic", topic),
    zap.Int64("partition", int64(partition)),
  )
}
