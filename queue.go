/**
 * Released under the MIT License.
 *
 * Copyright (c) 2019-2020 Miha Vrhovnik <miha.vrhovnik@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package disque

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// QueueDetails contains details for a specific Disque queue
type QueueDetails struct {
	QueueName  string
	Length     int64
	Age        int64
	Idle       int64
	Blocked    int64
	ImportFrom []string
	ImportRate int64
	JobsIn     int64
	JobsOut    int64
	Pause      string
}

// QueueLength will return the number of items in specified queue
func (me *Disque) QueueLength(ctx context.Context, queueName string) (queueLength int64, err error) {
	cmd := redis.NewIntCmd(ctx, "QLEN", queueName)

	if err := me.r.Process(ctx, cmd); err != nil {
		return 0, err
	}

	return cmd.Val(), nil
}

// QueueList will return a list of all Disque queues
func (me *Disque) QueueList(ctx context.Context) (queues []string, err error) {
	// ScanCmd or rather Iterator is case sensitive so we send this in lower case
	cmd := redis.NewScanCmd(ctx, me.r.Process, "qscan", "0")

	if err := me.r.Process(ctx, cmd); err != nil {
		return nil, err
	}

	iter := cmd.Iterator()
	for iter.Next(ctx) {
		queues = append(queues, iter.Val())
	}

	return queues, nil
}

// QueueStat will return the statistics for specified queue
func (me *Disque) QueueStat(ctx context.Context, queueName string) (stats *QueueDetails, err error) {
	cmd := redis.NewCmd(ctx, "QSTAT", queueName)

	err = me.r.Process(ctx, cmd)
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	info, ok := cmd.Val().([]interface{})
	if !ok {
		return nil, nil
	}

	stats = &QueueDetails{
		QueueName:  info[1].(string),
		Length:     info[3].(int64),
		Age:        info[5].(int64),
		Idle:       info[7].(int64),
		Blocked:    info[9].(int64),
		ImportFrom: doStrings(info[11].([]interface{})),
		ImportRate: info[13].(int64),
		JobsIn:     info[15].(int64),
		JobsOut:    info[17].(int64),
		Pause:      info[19].(string),
	}

	return stats, nil
}
