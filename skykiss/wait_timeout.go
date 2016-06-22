package skykiss

import (
	"sync"
	"time"
)

func WaitTimeout(c *sync.Cond, timeout time.Duration) {
	var t = time.AfterFunc(timeout, c.Broadcast)
	c.Wait()
	t.Stop()
}
