package telegram

import (
	"strings"
	"sync"
)

const errConnLimitExceeded = "cannot assign requested address"

var (
	mutex             sync.Mutex
	errIgnorePatterns []string
)

func AddErrIgnorePatterns(patterns ...string) {
	mutex.Lock()
	defer mutex.Unlock()
	errIgnorePatterns = append(errIgnorePatterns, patterns...)
}
func DefaultErrIgnorePatterns() {
	AddErrIgnorePatterns(errConnLimitExceeded)
}

func ConsiderErrShouldBeSent(
	err error,
	src, f string, extra any,
) (*TeleMsg, bool) {
	mutex.Lock()
	defer mutex.Unlock()
	for _, p := range errIgnorePatterns {
		if strings.Contains(err.Error(), p) {
			return nil, false
		}
	}
	return ErrorMsg(err.Error(), src, f, extra), true
}
