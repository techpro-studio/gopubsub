package pubsub

import (
	"fmt"
	"github.com/techpro-studio/gopubsub/abstract"
	"github.com/techpro-studio/gopubsub/amqp"
	"github.com/techpro-studio/gopubsub/google"
	"os"
	"strings"
)

const KEnvAMQPURL = "AMQP_URL"
const KEnvAMQPExchange = "AMQP_EXCHANGE"
const KEnvAMQPQueue = "AMQP_QUEUE"
const KEnvAMQPRoutingKey = "AMQP_ROUTING_KEY"
const KEnvAMQPListenerName = "AMQP_LISTENER_NAME"
const KEnvNeedAck = "PUBSUB_NEED_ACK"

const KEnvGoogleProjectId = "GOOGLE_PROJECT_ID"
const KEnvGooglePubSubAccountKeyB64 = "GOOGLE_PUBSUB_ACCOUNT_KEY_B64"
const KEnvGooglePubSubTopic = "GOOGLE_PUBSUB_TOPIC"
const KEnvGoogleSubscriberUseHttpProxy = "GOOGLE_SUBSCRIBER_USE_HTTP_PROXY"
const KEnvGoogleSubscriberHttpProxyPort = "GOOGLE_SUBSCRIBER_HTTP_PROXY_PORT"

const KEnvPubSubType = "PUBSUB_TYPE"

const TypeAMQP = "amqp"
const TypeGoogle = "google"

type Credentials interface {
	GetPubSubType() string
}

type GoogleCredentials struct {
	ProjectId              string `json:"project_id"`
	AccountKeyB64          string `json:"account_key_json"`
	SubscriberUseHttpProxy bool   `json:"subscriber_use_http_proxy"`
	HttpProxyPort          string `json:"http_proxy_port"`
	SubscriberTopic        string `json:"subscriber_topic"`
}

func (g GoogleCredentials) GetPubSubType() string {
	return TypeGoogle
}

type AmqpCredentials struct {
	AmqpUrl          string `json:"url"`
	AmqpExchange     string `json:"exchange"`
	AmqpQueue        string `json:"queue"`
	AmqpRoutingKey   string `json:"routing_key"`
	AmqpListenerName string `json:"listener_name"`
	NeedAck          bool   `json:"ack"`
}

func (a AmqpCredentials) GetPubSubType() string {
	return TypeAMQP
}

func GetCredentialsFromEnv(envPrefix string) (Credentials, error) {
	getEnv := func(key string) string {
		if envPrefix == "" {
			return os.Getenv(key)
		}
		return os.Getenv(fmt.Sprintf("%s_%s", envPrefix, key))
	}

	getBoolFromEnv := func(key string) bool {
		val := strings.ToLower(getEnv(key))
		return val == "1" || val == "true" || val == "yes"
	}

	pubSubType := getEnv(KEnvPubSubType)
	if pubSubType == "" {
		return nil, fmt.Errorf("%s environment variable not set", KEnvPubSubType)
	}
	if pubSubType == TypeAMQP {
		return AmqpCredentials{
			AmqpUrl:          getEnv(KEnvAMQPURL),
			AmqpExchange:     getEnv(KEnvAMQPExchange),
			AmqpQueue:        getEnv(KEnvAMQPQueue),
			AmqpRoutingKey:   getEnv(KEnvAMQPRoutingKey),
			AmqpListenerName: getEnv(KEnvAMQPListenerName),
			NeedAck:          getBoolFromEnv(KEnvNeedAck),
		}, nil

	} else if pubSubType == TypeGoogle {
		return GoogleCredentials{
			ProjectId:              getEnv(KEnvGoogleProjectId),
			AccountKeyB64:          getEnv(KEnvGooglePubSubAccountKeyB64),
			SubscriberUseHttpProxy: getBoolFromEnv(KEnvGoogleSubscriberUseHttpProxy),
			HttpProxyPort:          getEnv(KEnvGoogleSubscriberHttpProxyPort),
			SubscriberTopic:        getEnv(KEnvGooglePubSubTopic),
		}, nil
	}
	return nil, fmt.Errorf("%s environment variable is not correct, it should be either amqp or google", KEnvPubSubType)
}

func NewPublisherFromCredentials(credentials Credentials) (abstract.Publisher, error) {

	if credentials.GetPubSubType() == TypeGoogle {
		casted := credentials.(GoogleCredentials)
		if casted.ProjectId == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvGoogleProjectId)
		}
		if casted.AccountKeyB64 == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvGooglePubSubAccountKeyB64)
		}
		return google.NewPublisher(casted.AccountKeyB64, casted.ProjectId), nil
	} else {
		casted := credentials.(AmqpCredentials)
		if casted.AmqpUrl == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvAMQPURL)
		}
		if casted.AmqpExchange == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvAMQPExchange)
		}
		return amqp.NewPublisher(casted.AmqpUrl, casted.AmqpExchange), nil
	}
}

func NewSubscriberFromCredentials(credentials Credentials) (abstract.Subscriber, error) {
	if credentials.GetPubSubType() == TypeGoogle {
		casted, ok := credentials.(GoogleCredentials)
		if !ok {
			return nil, fmt.Errorf("invalid Google credentials")
		}
		if casted.SubscriberUseHttpProxy {
			port := casted.HttpProxyPort
			if port == "" {
				return nil, fmt.Errorf("%s environment variable not set", KEnvGoogleSubscriberHttpProxyPort)
			}
			return google.NewHttpProxySubscriber(port), nil
		}
		if casted.ProjectId == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvGoogleProjectId)
		}
		if casted.AccountKeyB64 == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvGooglePubSubAccountKeyB64)
		}
		if casted.SubscriberTopic == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvGooglePubSubTopic)
		}
		return google.NewDefaultSubscriber(casted.AccountKeyB64, casted.ProjectId, casted.SubscriberTopic), nil
	} else {
		casted, ok := credentials.(AmqpCredentials)
		if !ok {
			return nil, fmt.Errorf("invalid AMQP credentials")
		}
		if casted.AmqpUrl == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvAMQPURL)
		}
		if casted.AmqpExchange == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvAMQPExchange)
		}
		if casted.AmqpRoutingKey == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvAMQPRoutingKey)
		}
		if casted.AmqpListenerName == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvAMQPListenerName)
		}
		if casted.AmqpQueue == "" {
			return nil, fmt.Errorf("%s environment variable not set", KEnvAMQPQueue)
		}
		return amqp.NewSubscriber(casted.AmqpUrl, casted.AmqpQueue, casted.AmqpListenerName, casted.NeedAck, casted.AmqpExchange, casted.AmqpRoutingKey), nil
	}
}
