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
    "os"
    "time"
    "strconv"
    . "syscall"
)

type COLA struct {
    metafd    int
    bitmap    uint64
    blocks    *Blocks
    /* 57 because of log() */
    extents   [57]*Extent
    loadtime  int64
}

func NewCOLA(path string) (*COLA, bool) {
    err := Mkdir(path, S_IRALL | S_IWALL | S_IXALL)
    if err != nil {
        return nil, false
    }
    var cola = new(COLA)
    var mode int = O_WRONLY | O_CREAT
    cola.metafd, err = Open(path + "/meta", mode, S_IRALL | S_IWALL)
    if err != nil {
        return nil, false
    }
    n, err := Write(cola.metafd, uint64ToBytes(cola.bitmap))
    if err != nil || n != 8 {
        return nil, false
    }
    var ok bool
    cola.blocks, ok = NewBlocks(path + "/blx")
    if !ok {
        return nil, false
    }
    cola.loadtime = time.Now().Unix()
    return cola, true
}

func OpenCOLA(path string) (*COLA, bool) {
    var cola = new(COLA)
    var err error
    cola.metafd, err = Open(path + "/meta", O_RDWR, S_IRALL | S_IWALL)
    if err != nil {
        return nil, false
    }
    Seek(cola.metafd, -8, os.SEEK_END)
    var buffer = make([]byte, 8)
    n, err := Read(cola.metafd, buffer)
    if err != nil || n != 8 {
        return nil, false
    }
    cola.bitmap = bytesToUint64(buffer)
    var ok bool
    cola.blocks, ok = LoadBlocks(path + "/blx")
    if !ok {
        return nil, false
    }
    for i := 64; i > 0; i <<= 1 {
        if uint64(i) & cola.bitmap > 0 {
            extpath := path + "/ext_" + strconv.Itoa(i)
            cola.extents[log(i)], ok = OpenExtent(extpath)
        }
    }
    cola.loadtime = time.Now().Unix()
    return cola, true
}

func log(i int) int {
    var result int = 0
    for i >>= 7; i > 0; i >>= 1 {
        result++
    }
    return result
}
