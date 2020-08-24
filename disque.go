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
	"github.com/go-redis/redis/v8"
)

type Disque struct {
	r *redis.Client
}

// NewClient creates a new Disque client
func NewClient(o *redis.Options) *Disque {
	return &Disque{
		r: redis.NewClient(o),
	}
}

func (me *Disque) optionsToArguments(options map[string]string) (arguments []interface{}) {
	arguments = make([]interface{}, 0)
	for key, value := range options {
		if value == "true" {
			arguments = append(arguments, key)
		} else {
			arguments = append(arguments, key, value)
		}
	}
	return
}

func doStrings(in []interface{}) []string {
	s := make([]string, len(in))
	for i, v := range in {
		s[i] = v.(string)
	}

	return s
}
