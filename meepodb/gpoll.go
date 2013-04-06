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
    "net"
    . "syscall"
)

type GpollLoop struct {
    Lfd    int32
    State  *GpollState
    Ready  int
}

func GpollListen(addr string, maxConns int) (*GpollLoop, bool) {
    raddr, err := net.ResolveTCPAddr("tcp", addr)
    if err != nil {
        return nil, false
    }
    listen, err := net.ListenTCP("tcp", raddr)
    if err != nil {
        return nil, false
    }
    file, err := listen.File()
    if err != nil {
        return nil, false
    }
    fd := int32(file.Fd())

    state, ok := GpollCreate(maxConns + 1024)
    if !ok {
        return nil, false
    }
    ev := EpollEvent{ Events: EPOLLIN, Fd: fd }
    ok = GpollAdd(state, &ev)
    if !ok {
        return nil, false
    }
    return &GpollLoop{ fd, state, 0 }, true
}

func (loop *GpollLoop) Wait() {
    loop.Ready = GpollWait(loop.State)
}

func (loop *GpollLoop) AddEvent() bool {
    fd, _, err := Accept(int(loop.Lfd))
    if err != nil { return false }
    err = SetNonblock(fd, true)
    if err != nil { return false }
    ev := EpollEvent{ Events: EPOLLIN|(EPOLLET & 0xFFFFFFFF), Fd: int32(fd) }
    return GpollAdd(loop.State, &ev)
}

func (loop *GpollLoop) DelEvent(ev *EpollEvent) bool {
    return GpollDel(loop.State, ev)
}
