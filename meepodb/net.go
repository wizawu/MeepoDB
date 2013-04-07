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
    . "syscall"
)

var CLUSTER_TAG uint64

func StartServer(addr string) {
    gpoll, ok := GpollListen(addr, MAX_CONNS)
    if !ok {
        println("GpollListen on", addr, "failed.")
        return
    }
    var strg = NewStorage()
    for {
        gpoll.Wait()
        if gpoll.Ready == -1 {
            println("GpollWait failed.")
            return
        }
        for i, ev := range gpoll.State.Events[:gpoll.Ready] {
            if ev.Fd == gpoll.Lfd {
                ok = gpoll.AddEvent()
                if !ok {
                    println("Gpoll.AddEvent failed.")
                    return
                }
            } else {
                var sockfd = int(ev.Fd)
                code, tab, k, v := readRequest(sockfd)
                if code == ERR_CODE {
                    continue
                }
                switch code {
                    case GET_CODE:
                        v = strg.Get(tab, k)
                        head := EncodeHead(OK_CODE, 0, 0, uint64(len(v)))
                        n, err := Write(sockfd, head)
                        if err != nil || n != 8 {
                            println("Cannot reply", sockfd)
                            continue
                        }
                        if len(v) != 0 {
                            n, err = Write(sockfd, v)
                            if err != nil || n != len(v) {
                                println("Cannot reply", sockfd)
                            }
                        }
                    case SET_CODE:
                        var ok bool = strg.Set(tab, k, v)
                        if !ok {
                            /* Value is always long, so do not print it */
                            println("Cannot set", string(tab), string(k))
                        }
                    case DROP_CODE:
                        var ok bool = strg.Drop(tab)
                        if ok {
                            println("Drop", string(tab))
                        } else {
                            println("Cannot drop", string(tab))
                        }
                    case QUIT_CODE:
                        gpoll.DelEvent(&gpoll.State.Events[i])
                        Close(sockfd)
                        println("Close sockfd", sockfd)
                    default:
                        println("Unknown request")
                }
            }
        }
    }
}

func readRequest(sockfd int) (byte, []byte, []byte, []byte) {
    var head = make([]byte, 8)
    n, err := Read(sockfd, head)
    if err != nil || n != 8 {
        return ERR_CODE, nil, nil, nil
    }
    code, tlen, klen, vlen := DecodeHead(head)
    switch code {
        case GET_CODE:
            buffer := receiveN(sockfd, tlen + klen)
            if buffer != nil {
                return GET_CODE, buffer[:tlen], buffer[tlen:], nil
            }
        case SET_CODE:
            buffer := receiveN(sockfd, tlen + klen + vlen)
            if buffer != nil {
                return SET_CODE, buffer[:tlen], buffer[tlen : tlen + klen],
                       buffer[tlen + klen :]
            }
        case DROP_CODE:
            buffer := receiveN(sockfd, tlen)
            if buffer != nil {
                return DROP_CODE, buffer, nil, nil
            }
        case QUIT_CODE:
            return QUIT_CODE, nil, nil, nil
    }
    return ERR_CODE, nil, nil, nil
}

func receiveN(sockfd int, n uint64) []byte {
    var buffer = make([]byte, n)
    m, err := Read(sockfd, buffer)
    if err != nil || m != int(n) {
        return nil
    }
    return buffer
}
