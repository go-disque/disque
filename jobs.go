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
	"math"
	"time"

	"github.com/go-redis/redis/v8"
)

// Job represents a Disque job
type Job struct {
	ID            string
	Data          string
	QueueName     string
	NackCount     int64
	DeliveryCount int64
}

// JobDetails contains details for a specific Disque job
type JobDetails struct {
	ID            string
	Data          string
	QueueName     string
	NackCount     int64
	DeliveryCount int64

	State             string
	ReplicationFactor int
	TTL               time.Duration
	CreatedAt         time.Time
	Delay             time.Duration
	Retry             time.Duration
	NodesDelivered    []string
	NodesConfirmed    []string
	NextRequeueWithin time.Duration
	NextAwakeWithin   time.Duration
}

// AddJob adds a job onto a Disque
//
// ADDJOB queue_name job <ms-timeout>
//    [REPLICATE <count>]
//    [DELAY <sec>]
//    [RETRY <sec>]
//    [TTL <sec>]
//    [MAXLEN <count>]
//    [ASYNC]
//
// Example:
//     options := make(map[string]string)
//     options["ms-timeout"] = msec
//     options["TTL"] = "600"
//     options["ASYNC"] = "true"
//     d.AddJob("queue_name", "data", options)
func (me *Disque) AddJob(ctx context.Context, queueName string, data string, options map[string]string) (ID string, err error) {
	var cmd *redis.StringCmd
	timeout := "0"
	args := []interface{}{"ADDJOB", queueName, data}
	if val, ok := options["ms-timeout"]; ok {
		timeout = val
		delete(options, "ms-timeout")
	}
	args = append(args, timeout)

	if len(options) > 0 {
		args = append(args, me.optionsToArguments(options)...)
	}

	cmd = redis.NewStringCmd(ctx, args...)
	err = me.r.Process(ctx, cmd)
	ID = cmd.Val()

	return
}

// AckJob command acknowledges the execution of one or more jobs via job IDs.
func (me *Disque) AckJob(ctx context.Context, ID ...string) (err error) {
	err = me.call(ctx, "ACKJOB", ID...)
	return
}

// FastAckJob command acknowledges the execution of one or more jobs via job IDs.
func (me *Disque) FastAckJob(ctx context.Context, ID ...string) (err error) {
	err = me.call(ctx, "FASTACK", ID...)
	return
}

// NackJob command tells Disque to put the job back in the queue ASAP
func (me *Disque) NackJob(ctx context.Context, ID ...string) (err error) {
	err = me.call(ctx, "NACK", ID...)
	return
}

// DeleteJob completely delete jobs from the queue
func (me *Disque) DeleteJob(ctx context.Context, ID ...string) (err error) {
	err = me.call(ctx, "DELJOB", ID...)
	return
}

// EnqueueJob command queues jobs if not already queued one or more jobs via job IDs.
func (me *Disque) EnqueueJob(ctx context.Context, ID ...string) (err error) {
	err = me.call(ctx, "ENQUEUE", ID...)
	return
}

// DequeueJob command removes one or more jobs via job IDs from the queue.
func (me *Disque) DequeueJob(ctx context.Context, ID ...string) (err error) {
	err = me.call(ctx, "DEQUEUE", ID...)
	return
}

// WorkingJob command The next delivery is postponed for the job retry time, however the command works in a best effort
// way since there is no way to guarantee during failures that another node in a different network partition won't perform
// a delivery of the same job.
// Another limitation of the WORKING command is that it cannot be sent to nodes not knowing about this particular job.
// In such a case the command replies with a NOJOB error. Similarly, if the job is already acknowledged an error is returned.
// Note that the WORKING command is refused by Disque nodes if 50% of the job time to live has already elapsed.
// This limitation makes Disque safer since usually the retry time is much smaller than the time-to-live of a job,
// so it can't happen that a set of broken workers monopolize a job with WORKING and never process it.
// After 50% of the TTL has elapsed, the job will be delivered to other workers anyway.
//
// Note that WORKING returns the number of seconds you (likely) postponed the message visibility for other workers
// (the command basically returns the retry time of the job)
func (me *Disque) WorkingJob(ctx context.Context, ID string) (postponed int64, err error) {
	cmd := redis.NewIntCmd(ctx, "WORKING", ID)

	err = me.r.Process(ctx, cmd)
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}

	return cmd.Val(), nil
}

// GetJobDetails will retrieve multiple jobs from a Disque queue.
func (me *Disque) GetJobDetails(ctx context.Context, ID string) (details *JobDetails, err error) {
	cmd := redis.NewCmd(ctx, "SHOW", ID)

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

	return me.parseShow(info), nil
}

// GetJob a single job from a Disque queue.
func (me *Disque) GetJob(ctx context.Context, timeout time.Duration, queueName ...string) (job *Job, err error) {
	jobs, err := me.GetJobs(ctx, 1, timeout, queueName...)
	if err != nil {
		return nil, err
	}

	if len(jobs) > 0 {
		return jobs[0], nil
	}

	return nil, nil
}

// GetJobs will retrieve multiple jobs from a Disque queue.
func (me *Disque) GetJobs(ctx context.Context, count int, timeout time.Duration, queueName ...string) (jobs []*Job, err error) {
	jobs = make([]*Job, 0)
	params := make([]interface{}, len(queueName))
	for key, val := range queueName {
		params[key] = val
	}
	params = append([]interface{}{"GETJOB", "TIMEOUT", int64(timeout.Seconds() * 1000), "COUNT", count, "WITHCOUNTERS", "FROM"}, params...)

	cmd := redis.NewSliceCmd(ctx, params...)

	if err := me.r.Process(ctx, cmd); err != nil {
		if err == redis.Nil {
			return jobs, nil
		}
		return nil, err
	}

	for _, oj := range cmd.Val() {
		job, ok := oj.([]interface{})
		if !ok {
			continue
		}
		j := &Job{
			QueueName:     job[0].(string),
			ID:            job[1].(string),
			Data:          job[2].(string),
			NackCount:     job[4].(int64),
			DeliveryCount: job[6].(int64),
		}
		jobs = append(jobs, j)
	}

	return jobs, err
}

// JobScan scans queue for jobs
// count is the number of jobs to return -1 == all, 0 == default (currently 100), > 0 count
// WARNING: idOnly must currently be set to true, otherwise an error is reported
func (me *Disque) JobScan(ctx context.Context, queueName string, count int, idOnly bool) (jobs []*JobDetails, err error) {
	reply := "all"
	if idOnly {
		reply = "id"
	}

	page := 100
	if count > 0 && count < 100 {
		page = count
	}
	if count < 0 {
		count = math.MaxInt32
	} else if count == 0 {
		count = 100
	}

	cmd := redis.NewScanCmd(ctx, me.r.Process, "JSCAN", "0", "COUNT", page, "QUEUE", queueName, "REPLY", reply)

	err = me.r.Process(ctx, cmd)
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	page = 0
	iter := cmd.Iterator()
	for iter.Next(ctx) {
		page++
		if idOnly {
			jobs = append(jobs, &JobDetails{
				ID:        iter.Val(),
				QueueName: queueName,
			})
		}

		if page > count {
			break
		}
	}

	return jobs, err
}

func (me *Disque) call(ctx context.Context, command string, ID ...string) (err error) {
	args := make([]interface{}, 1+len(ID))
	args[0] = command
	for i, v := range ID {
		args[i+1] = v
	}

	cmd := redis.NewStringCmd(ctx, args...)
	err = me.r.Process(ctx, cmd)

	return
}

func (me *Disque) parseShow(info []interface{}) *JobDetails {
	return &JobDetails{
		ID:                info[1].(string),
		QueueName:         info[3].(string),
		State:             info[5].(string),
		ReplicationFactor: int(info[7].(int64)),
		TTL:               time.Duration(info[9].(int64)) * time.Second,
		CreatedAt:         time.Unix(0, info[11].(int64)),
		Delay:             time.Duration(info[13].(int64)) * time.Second,
		Retry:             time.Duration(info[15].(int64)) * time.Second,
		NackCount:         info[17].(int64),
		DeliveryCount:     info[19].(int64),
		NodesDelivered:    doStrings(info[21].([]interface{})),
		NodesConfirmed:    doStrings(info[23].([]interface{})),
		NextRequeueWithin: time.Duration(info[25].(int64)/1000) * time.Second,
		NextAwakeWithin:   time.Duration(info[27].(int64)/1000) * time.Second,
		Data:              info[29].(string),
	}
}
