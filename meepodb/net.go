/*
 *  Copyright (c) 2013 Hualiang Wu <wizawu@gmail.com>
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to
 *  deal in the Software without restriction, including without limitation the
 *  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 *  sell copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *  IN THE SOFTWARE.
 */

package meepodb

import (
//  "bytes"
    "strconv"
//  . "syscall"
)

var CLUSTER_TAG uint64

func StartServer(port int) {
    var addr string = "127.0.0.1:" + strconv.Itoa(port)
    gpoll, ok := GpollListen(addr, MAX_CONNS)
    if !ok {
        println("GpollListen on", addr, "failed.")
        return
    }
    for {
        gpoll.Wait()
        if gpoll.Ready == -1 {
            println("GpollWait failed.")
            return
        }
        for _, ev := range gpoll.State.Events[:gpoll.Ready] {
            if ev.Fd == gpoll.Lfd {
                ok = gpoll.AddEvent()
                if !ok {
                    println("Gpoll.AddEvent failed.")
                    return
                }
            } else {
//              var sockfd = int(ev.Fd)
            }
        }
    }
}
