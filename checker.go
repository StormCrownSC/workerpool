package pool

import (
	"errors"
	"time"
)

type ScalingType uint8

const (
	ScalingTypeQueue  ScalingType = 0
	ScalingTypeWorker ScalingType = 1

	maxLoad              = 0.7
	minLoad              = 0.3
	workersToAddRatio    = 0.2
	workersToRemoveRatio = 0.1
	percent              = 100
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
	if conf.Enable {
		if conf.MinWorkers <= 0 {
			return conf, errors.New("min workers count must be greater than 0")
		}

		if conf.MaxWorkers < conf.MinWorkers {
			return conf, errors.New("max workers count must be greater or equel than min workers")
		}

		if conf.Type > 1 {
			conf.Type = 0
		}

		if conf.MaxLoadPercent == 0 {
			conf.MaxLoadPercent = maxLoad
		} else {
			conf.MaxLoadPercent /= percent
		}

		if conf.MinLoadPercent == 0 {
			conf.MinLoadPercent = minLoad
		} else {
			conf.MinLoadPercent /= percent
		}

		if conf.WorkersToAddRatioPercent == 0 {
			conf.WorkersToAddRatioPercent = workersToAddRatio
		} else {
			conf.WorkersToAddRatioPercent /= percent
		}

		if conf.WorkersToRemoveRatioPercent == 0 {
			conf.WorkersToRemoveRatioPercent = workersToRemoveRatio
		} else {
			conf.WorkersToRemoveRatioPercent /= percent
		}

		if conf.Interval == 0 {
			conf.Interval = time.Second
		}

		conf.Logging = checkLoggingConf(conf.Logging)
	}

	return conf, nil
}

func checkLoggingConf(conf Logging) Logging {
	if conf.Enable == true {
		if conf.Interval == 0 {
			conf.Interval = time.Minute
		}
	}
	return conf
}
