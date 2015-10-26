package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
	"github.com/samuel/go-zookeeper/zk"
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
		return sarama.ConfigurationError("ZookeeperTimeout should have a duration > 0")
	}

	if cgc.Offsets.CommitInterval <= 0 {
		return sarama.ConfigurationError("CommitInterval should have a duration > 0")
	}

	if cgc.Offsets.Initial != sarama.OffsetOldest && cgc.Offsets.Initial != sarama.OffsetNewest {
		return errors.New("Offsets.Initial should be sarama.OffsetOldest or sarama.OffsetNewest.")
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
}

// Connects to a consumer group, using Zookeeper for auto-discovery
func JoinConsumerGroup(name string, topics []string, zookeeper []string, config *Config) (*ConsumerGroup, error) {

	config.ClientID = name
	consumerGroup := &ConsumerGroup {
		config: config,
		zookeeper: zookeeper,
		topics: topics,
	}
	err := consumerGroup.Load()
	return consumerGroup, err
}

func (cg *ConsumerGroup) Load() error {
	var kz *kazoo.Kazoo
	var err error
	if kz, err = kazoo.NewKazoo(cg.zookeeper, cg.config.Zookeeper); err != nil {
		return err
	}

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
		log.Fatal("Failed to check existence of consumergroup: %s\n", err)
		consumer.Close()
		kz.Close()
		return err
	} else if !exists {
		log.Info("Consumergroup %s does not exist, creating...\n", cg.group.Name)
		if err := cg.group.Create(); err != nil {
			log.Fatal("Failed to create consumergroup in Zookeeper: %s\n", err)
			consumer.Close()
			kz.Close()
			return err
		}
	}

	if err := cg.instance.Register(cg.topics); err != nil {
		log.Fatal("Failed to create consumer instance: %s\n", err)
		return err
	} else {
		log.Info("Consumer instance registered\n")
	}

	offsetConfig := OffsetManagerConfig {
		CommitInterval: cg.config.Offsets.CommitInterval,
		EnableAutoCommit: cg.config.EnableOffsetAutoCommit,
	}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig)
	go cg.topicListConsumer(cg.topics)

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

func (cg *ConsumerGroup) GetBatchOfMessages(batchSize int) []string {
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
				log.Critical("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n",
					message.Topic,
					message.Partition,
					offsets[message.Topic][message.Partition] + 1,
					message.Offset,
					message.Offset - offsets[message.Topic][message.Partition] + 1,
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


func (cg *ConsumerGroup) reload() error {
	cg.reloadMutex.Lock()
	defer cg.reloadMutex.Unlock()
	cg.singleReload.Do(func() {
		log.Debug("Closing down old connections.")		
		err := cg.Close()
		if err != nil {
			log.Critical("Failed to close consumergroup due to %+v", err)			
		}
		cg.Load()
	})
	return nil
}

func (cg *ConsumerGroup) Close() error {	
	shutdownError := AlreadyClosing
	cg.singleShutdown.Do(func() {
		defer cg.kazoo.Close()

		shutdownError = nil
		close(cg.stopper)
		cg.wg.Wait()
		if err := cg.offsetManager.Close(); err != nil {
			log.Critical("FAILED closing the offset manager: %s!\n", err)
		}
		if shutdownError = cg.instance.Deregister(); shutdownError != nil {
			log.Warning("FAILED deregistering consumer instance: %s!\n", shutdownError)
		} else {
			log.Info("Deregistered consumer instance %s.\n", cg.instance.ID)
		}
		if shutdownError = cg.consumer.Close(); shutdownError != nil {
			log.Critical("FAILED closing the Sarama client: %s\n", shutdownError)
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

func (cg *ConsumerGroup) CommitOffsets() error {
	cg.reloadMutex.Lock()
	defer cg.reloadMutex.Unlock()
	return cg.offsetManager.CommitOffsets()
}

func (cg *ConsumerGroup) CommitUpto(message *sarama.ConsumerMessage) error {
	cg.offsetManager.MarkAsProcessed(message.Topic, message.Partition, message.Offset)
	return nil
}

func (cg *ConsumerGroup) topicListConsumer(topics []string) {
	for {
		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.group.WatchInstances()
		if err != nil {
			log.Fatal("FAILED to get list of registered consumer instances: %s\n", err)
			return
		}

		cg.consumers = consumers
		log.Info("Currently registered consumers: %d\n", len(cg.consumers))
		
		stopper := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(1)
			go cg.topicConsumer(topic, cg.messages, cg.errors, stopper)
		}

		select {
		case <-cg.stopper:
			close(stopper)
			return

		case event := <-consumerChanges:
			if event.Err == zk.ErrSessionExpired {
				log.Info("Session was expired, reloading consumer.")				
				go cg.reload()
				<- cg.stopper
				close(stopper)
				return
			} else {
				log.Info("Triggering rebalance due to consumer list change\n")
				close(stopper)
				cg.wg.Wait()
			}
		}
	}
}

func (cg *ConsumerGroup) topicConsumer(topic string, messages chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, stopper <-chan struct{}) {
	defer cg.wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	log.Info("%s :: Started topic consumer\n", topic)

	// Fetch a list of partition IDs
	partitions, err := cg.kazoo.Topic(topic).Partitions()
	if err != nil {
		log.Fatal("%s :: FAILED to get list of partitions: %s\n", topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	partitionLeaders, err := retrievePartitionLeaders(partitions)
	if err != nil {
		log.Fatal("%s :: FAILED to get leaders of partitions: %s\n", topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitionLeaders)
	myPartitions := dividedPartitions[cg.instance.ID]
	log.Info("%s :: Claiming %d of %d partitions", topic, len(myPartitions), len(partitionLeaders))
	// Consume all the assigned partitions
	var wg sync.WaitGroup
	myPartitionsStr := ""
	for _, pid := range myPartitions {
		myPartitionsStr += fmt.Sprintf("%d ", pid.ID)
		wg.Add(1)
		go cg.partitionConsumer(topic, pid.ID, messages, errors, &wg, stopper)
	}
	log.Info("My partitions are %s\n", myPartitionsStr)
	wg.Wait()
	log.Info("%s :: Stopped topic consumer\n", topic)
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, messages chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, wg *sync.WaitGroup, stopper <-chan struct{}) {
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
			log.Warning("%s/%d :: FAILED to claim the partition: %s\n", topic, partition, err)
			return
		}
	}
	defer cg.instance.ReleasePartition(topic, partition)

	nextOffset, err := cg.offsetManager.InitializePartition(topic, partition)

	if err != nil {
		log.Error("%s/%d :: FAILED to determine initial offset: %s\n", topic, partition, err)
		return
	}

	if nextOffset >= 0 {
		log.Info("%s/%d :: Partition consumer starting at offset %d.\n", topic, partition, nextOffset)
	} else {
		nextOffset = cg.config.Offsets.Initial
		if nextOffset == sarama.OffsetOldest {
			log.Info("%s/%d :: Partition consumer starting at the oldest available offset.\n", topic, partition)
		} else if nextOffset == sarama.OffsetNewest {
			log.Info("%s/%d :: Partition consumer listening for new messages only.\n", topic, partition)
		}
	}

	consumer, err := cg.consumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		log.Warning("%s/%d :: Partition consumer offset out of Range.\n", topic, partition)
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if cg.config.Offsets.Initial == sarama.OffsetOldest {
			nextOffset = sarama.OffsetOldest
			log.Info("%s/%d :: Partition consumer offset reset to oldest available offset.\n", topic, partition)
		} else {
			nextOffset = sarama.OffsetNewest
			log.Info("%s/%d :: Partition consumer offset reset to newest available offset.\n", topic, partition)
		}
		// retry the consumePartition with the adjusted offset
		consumer, err = cg.consumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
		log.Fatal("%s/%d :: FAILED to start partition consumer: %s\n", topic, partition, err)
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

	log.Info("%s/%d :: Stopping partition consumer at offset %d\n", topic, partition, lastOffset)
	if err = cg.offsetManager.FinalizePartition(topic, partition, lastOffset, cg.config.Offsets.ProcessingTimeout); err != nil {
		log.Fatal("%s/%d :: %s\n", topic, partition, err)
	}
	log.Info("%s/%d :: Successfully stoped partition.", topic, partition)
}
