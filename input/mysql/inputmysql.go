package inputmysql

import (
	"context"
	"github.com/tsaikd/KDGoLib/errutil"
	codecjson "github.com/tsaikd/gogstash/codec/json"
	"github.com/tsaikd/gogstash/config"
	"gopkg.in/redis.v5"
	"time"
)

// ModuleName is the name used in config file
const ModuleName = "mysql"

// ErrorTag tag added to event when process module failed
const ErrorTag = "gogstash_input_mysql_error"

// InputConfig holds the configuration json fields and internal objects
type InputConfig struct {
	config.InputConfig
	Host        string `json:"host"`        // redis server host:port, default: "localhost:6379"
	Key         string `json:"key"`         // where to get data, default: "gogstash"
	Connections int    `json:"connections"` // maximum number of socket connections, default: 10
	BatchCount  int    `json:"batch_count"` // The number of events to return from Redis using EVAL, default: 125

	// BlockingTimeout used for set the blocking timeout interval in redis BLPOP command
	// Defaults to 600s
	BlockingTimeout string `json:"blocking_timeout,omitempty"` // automatically
	blockingTimeout time.Duration

	client         *redis.Client
	batchScriptSha string
}

// DefaultInputConfig returns an InputConfig struct with default values
func DefaultInputConfig() InputConfig {
	return InputConfig{
		InputConfig: config.InputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Host:            "localhost:3306",
		Key:             "gogstash",
		Connections:     10,
		BatchCount:      125,
		BlockingTimeout: "600s",
	}
}

// errors
var (
	ErrorPingFailed = errutil.NewFactory("ping redis server failed")
)

// InitHandler initialize the input plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeInputConfig, error) {
	conf := DefaultInputConfig()
	err := config.ReflectConfig(raw, &conf)
	if err != nil {
		return nil, err
	}

	conf.blockingTimeout, err = time.ParseDuration(conf.BlockingTimeout)
	if err != nil {
		return nil, err
	}

	conf.client = redis.NewClient(&redis.Options{
		Addr:     conf.Host,
		PoolSize: conf.Connections,
	})
	conf.client = conf.client.WithContext(ctx)

	if _, err := conf.client.Ping().Result(); err != nil {
		return nil, ErrorPingFailed.New(err)
	}

	if conf.BatchCount > 1 {
		err = conf.loadBatchScript()
		if err != nil {
			return nil, err
		}
	}

	conf.Codec, err = config.GetCodecDefault(ctx, *raw, codecjson.ModuleName)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}
