package test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		writers int
		readers int
		count   int
	}{
		{
			name:    "w1r1c1k",
			writers: 1,
			readers: 1,
			count:   1000,
		}, {
			name:    "w5r1c1k",
			writers: 5,
			readers: 1,
			count:   1000,
		}, {
			name:    "w10r1c1k",
			writers: 10,
			readers: 1,
			count:   1000,
		}, {
			name:    "w20r1c1k",
			writers: 20,
			readers: 1,
			count:   1000,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cl, _ := SetupForTesting(t)

			writes := test.count * test.writers

			keyCh := make(chan string, writes)
			var wg sync.WaitGroup

			wg.Add(test.writers)
			t0 := time.Now()

			for i := 0; i < test.writers; i++ {
				go func() {
					keys := make(map[string]bool)
					for i := 0; i < test.count; i++ {
						key := uniqKey(keys)
						err := cl.Set(ctx, key, []byte(genRand(255)))
						jtest.RequireNil(t, err)
						keyCh <- key
					}
					wg.Done()
				}()
			}

			for i := 0; i < test.readers; i++ {
				go func() {
					for key := range keyCh {
						_, err := cl.Get(ctx, key)
						if err != nil && (strings.Contains(strings.ToLower(err.Error()), "canceled") || strings.Contains(strings.ToLower(err.Error()), "closing")) {
							return
						}
						jtest.RequireNil(t, err)
					}
				}()
			}

			wg.Wait()

			reads := writes - len(keyCh)
			delta := time.Since(t0)
			wps := float64(writes) / delta.Seconds()
			rps := float64(reads) / delta.Seconds()

			fmt.Printf("%s: Done in %v, wps=%v, rps=%v\n", test.name, delta, wps, rps)
		})
	}
}
