package pool

import (
	"errors"
	"time"
)

type ScalingType uint8

const (
	ScalingTypeQueue  ScalingType = 0
	ScalingTypeWorker ScalingType = 1

	defaultMaxLoadPercent       = 70
	defaultMinLoadPercent       = 30
	defaultWorkersToAddRatio    = 20
	defaultWorkersToRemoveRatio = 10
	percent                     = 100
)

func checkParams(conf Config) (Config, error) {
	if conf.WorkersCount <= 0 {
		return conf, errors.New("workers count must be greater than 0")
	}

	if conf.QueueSize <= 0 {
		return conf, errors.New("queue size must be greater than 0")
	}

	if conf.StopTimeout == 0 {
		conf.StopTimeout = 5 * time.Second
	}

	var err error
	conf.ScalingConfig, err = checkScalingConfig(conf.ScalingConfig)
	if err != nil {
		return conf, err
	}

	return conf, nil
}

func checkScalingConfig(conf ScalingConfig) (ScalingConfig, error) {
	if !conf.Enable {
		return conf, nil
	}

	if conf.MinWorkers <= 0 {
		return conf, errors.New("min workers count must be greater than 0")
	}

	if conf.MaxWorkers < conf.MinWorkers {
		return conf, errors.New("max workers count must be greater or equal than min workers")
	}

	if conf.Type > ScalingTypeWorker {
		conf.Type = ScalingTypeQueue
	}

	if conf.MaxLoadPercent == 0 {
		conf.MaxLoadPercent = defaultMaxLoadPercent
	}
	conf.MaxLoadPercent /= percent

	if conf.MinLoadPercent == 0 {
		conf.MinLoadPercent = defaultMinLoadPercent
	}
	conf.MinLoadPercent /= percent

	if conf.WorkersToAddRatioPercent == 0 {
		conf.WorkersToAddRatioPercent = defaultWorkersToAddRatio
	}
	conf.WorkersToAddRatioPercent /= percent

	if conf.WorkersToRemoveRatioPercent == 0 {
		conf.WorkersToRemoveRatioPercent = defaultWorkersToRemoveRatio
	}
	conf.WorkersToRemoveRatioPercent /= percent

	if conf.Interval <= 0 {
		conf.Interval = time.Second
	}

	conf.Logging = checkLoggingConf(conf.Logging)

	return conf, nil
}

func checkLoggingConf(conf Logging) Logging {
	if conf.Enable && conf.Interval <= 0 {
		conf.Interval = time.Minute
	}
	return conf
}
