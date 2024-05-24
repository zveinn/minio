package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
	saramatls "github.com/IBM/sarama/tools/tls"
	xnet "github.com/minio/pkg/v2/net"
)

type KafkaTrgt struct {
	kconfig Config

	client   sarama.Client
	producer sarama.SyncProducer
	config   *sarama.Config
}

func (k *KafkaTrgt) Write(data []byte) (n int, err error) {
	// if len(k.client.Brokers()) < 1 {
	// 	// Refer https://github.com/IBM/sarama/issues/1341
	// 	return 0, errors.New("no brokers online")
	// }

	_, _, err = k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: k.kconfig.Topic,
		Value: sarama.ByteEncoder(data),
	})
	return 0, err
}

// Init initialize kafka target
func NewKafaTarget(ctx context.Context, config *Config) (T *KafkaTrgt, err error) {
	T = new(KafkaTrgt)

	if len(config.Brokers) == 0 {
		return nil, errors.New("no broker address found")
	}

	for _, b := range config.Brokers {
		if _, err := xnet.ParseHost(b.String()); err != nil {
			return nil, err
		}
	}

	sconfig := sarama.NewConfig()
	if config.Version != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(config.Version)
		if err != nil {
			return nil, err
		}
		sconfig.Version = kafkaVersion
	}

	sconfig.Net.KeepAlive = 60 * time.Second
	sconfig.Net.SASL.User = config.SASL.User
	sconfig.Net.SASL.Password = config.SASL.Password
	initScramClient(*config, sconfig) // initializes configured scram client.
	sconfig.Net.SASL.Enable = config.SASL.Enable

	tlsConfig, err := saramatls.NewConfig(config.TLS.ClientTLSCert, config.TLS.ClientTLSKey)
	if err != nil {
		return nil, err
	}

	sconfig.Net.TLS.Enable = config.TLS.Enable
	sconfig.Net.TLS.Config = tlsConfig
	sconfig.Net.TLS.Config.InsecureSkipVerify = config.TLS.SkipVerify
	sconfig.Net.TLS.Config.ClientAuth = config.TLS.ClientAuth
	sconfig.Net.TLS.Config.RootCAs = config.TLS.RootCAs

	// These settings are needed to ensure that kafka client doesn't hang on brokers
	// refer https://github.com/IBM/sarama/issues/765#issuecomment-254333355
	sconfig.Producer.Retry.Max = 2
	sconfig.Producer.Retry.Backoff = (10 * time.Second)
	sconfig.Producer.Return.Successes = true
	sconfig.Producer.Return.Errors = true
	sconfig.Producer.RequiredAcks = 1
	sconfig.Producer.Timeout = (10 * time.Second)
	sconfig.Net.ReadTimeout = (10 * time.Second)
	sconfig.Net.DialTimeout = (10 * time.Second)
	sconfig.Net.WriteTimeout = (10 * time.Second)
	sconfig.Metadata.Retry.Max = 1
	sconfig.Metadata.Retry.Backoff = (10 * time.Second)
	sconfig.Metadata.RefreshFrequency = (15 * time.Minute)

	T.config = sconfig

	var brokers []string
	for _, broker := range config.Brokers {
		brokers = append(brokers, broker.String())
	}

	client, err := sarama.NewClient(brokers, sconfig)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	T.client = client
	T.producer = producer

	return
}
