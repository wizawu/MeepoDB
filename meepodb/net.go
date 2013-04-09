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
var buffer = make([]byte, 8 + MAX_TABLE_NAME_LEN + MAX_KEY_LEN + MAX_VALUE_LEN)

func SetKeepAlive(sockfd, v int) error {
    return SetsockoptInt(sockfd, SOL_SOCKET, SO_KEEPALIVE, v)
}

func SetNoDelay(sockfd int) error {
    return SetsockoptInt(sockfd, IPPROTO_TCP, TCP_NODELAY, 1)
}

func SetLinger(sockfd int, sec int) error {
    return SetsockoptInt(sockfd, SOL_SOCKET, SO_LINGER, sec)
}

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
                gpoll.AddEvent()
            } else {
                var sockfd = int(ev.Fd)
                code, tab, k, v := readRequest(sockfd)
                if code == ERR_CODE {
                    println("unknown request")
                    continue
                }
                switch code {
                    case GET_CODE:
                        v = strg.Get(tab, k)
                        head := EncodeHead(OK_CODE, 0, 0, uint64(len(v)))
                        n, err := Write(sockfd, head)
                        if err != nil || n != 8 {
                            println("cannot reply", sockfd)
                            continue
                        }
                        if len(v) != 0 {
                            n, err = Write(sockfd, v)
                            if err != nil || n != len(v) {
                                println("cannot reply", sockfd)
                            }
                        }
                    case SET_CODE:
                        var ok bool = strg.Set(tab, k, v)
                        if !ok {
                            /* Value is always long, so do not print it */
                            println("cannot SET", string(tab), string(k))
                        }
                    case DROP_CODE:
                        var ok bool = strg.Drop(tab)
                        if ok {
                            println("DROP", string(tab))
                        } else {
                            println("cannot DROP", string(tab))
                        }
                    case QUIT_CODE:
                        gpoll.DelEvent(&gpoll.State.Events[i])
                        Close(sockfd)
                        println("client", sockfd, "QUIT")
                }
            }
        }
    }
}

func readRequest(sockfd int) (byte, []byte, []byte, []byte) {
    n, err := Read(sockfd, buffer)
    if err != nil || n < 8 {
        return ERR_CODE, nil, nil, nil
    }
    code, tlen, klen, vlen := DecodeHead(buffer[:8])
    body := buffer[8 : n]
    switch code {
        case GET_CODE:
            if n != int(8 + tlen + klen) {
                return ERR_CODE, nil, nil, nil
            }
            return GET_CODE, body[:tlen], body[tlen:], nil
        case SET_CODE:
            if n != int(8 + tlen + klen + vlen) {
                return ERR_CODE, nil, nil, nil
            }
            return SET_CODE, body[:tlen], body[tlen : tlen + klen],
                   body[tlen + klen :]
        case DROP_CODE:
            if n != int(8 + tlen) {
                return ERR_CODE, nil, nil, nil
            }
            return DROP_CODE, body, nil, nil
        case QUIT_CODE:
            if n != 8 {
                return ERR_CODE, nil, nil, nil
            }
            return QUIT_CODE, nil, nil, nil
    }
    return ERR_CODE, nil, nil, nil
}
