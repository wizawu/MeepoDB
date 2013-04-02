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
    MetaFd    int
    Bitmap    uint64
    blocks    *Blocks
    /* 57 because of log() */
    extents   [57]*Extent
    LoadTime  int64
    Path      string
}

func (cola *COLA) Get(key []byte) []byte {
    var value []byte
    value = cola.blocks.Get(key)
    if value != nil {
        return value
    }
    for i := 64; i > 0; i <<= 1 {
        if uint64(i) & cola.Bitmap > 0 {
            j := cola.extents[log(i)].Find(key)
            if j >= 0 {
                _, value = cola.extents[log(i)].Record(uint64(j))
                return value
            }
        }
    }
    return nil
}

func (cola *COLA) Set(key, value []byte) bool {
    return cola.blocks.Set(key, value)
}

func (cola *COLA) PushDown() bool {
    ok := BlocksToExtent(cola.Path + "/ext_64.1", cola.blocks.records)
    if !ok {
        return false
    }
    for i := 64; i > 0; i <<= 1 {
        if uint64(i) & cola.Bitmap == 0 {
            oldpath := cola.Path + "/ext_" + strconv.Itoa(i) + ".1"
            newpath := cola.Path + "/ext_" + strconv.Itoa(i)
            err := Rename(oldpath, newpath)
            if err != nil {
                return false
            }
            cola.Bitmap |= uint64(i)
            cola.extents[log(i)], ok = OpenExtent(newpath)
            if !ok {
                return false
            }
            break
        }
        oldpath := cola.Path + "/ext_" + strconv.Itoa(i) + ".1"
        newpath := cola.Path + "/ext_" + strconv.Itoa(i << 1) + ".1"
        ext, ok := OpenExtent(oldpath)
        if !ok {
            return false
        }
        ok = MergeExtents(newpath, cola.extents[log(i)], ext)
        if !ok {
            return false
        }
        cola.extents[log(i)].Free()
        ext.Free()
        cola.Bitmap &= ^uint64(i)
    }
    n, err := Write(cola.MetaFd, uint64ToBytes(cola.Bitmap))
    if err != nil || n != 8 {
        return false
    }
    cola.blocks.Close()
    cola.blocks, ok = NewBlocks(cola.Path + "/blx")
    return ok
}

func NewCOLA(path string) (*COLA, bool) {
    err := Mkdir(path, S_IRALL | S_IWALL | S_IXALL)
    if err != nil {
        return nil, false
    }
    var cola = new(COLA)
    var mode int = O_WRONLY | O_CREAT
    cola.MetaFd, err = Open(path + "/meta", mode, S_IRALL | S_IWALL)
    if err != nil {
        return nil, false
    }
    n, err := Write(cola.MetaFd, uint64ToBytes(cola.Bitmap))
    if err != nil || n != 8 {
        return nil, false
    }
    var ok bool
    cola.blocks, ok = NewBlocks(path + "/blx")
    if !ok {
        return nil, false
    }
    cola.LoadTime = time.Now().Unix()
    cola.Path = path
    return cola, true
}

func OpenCOLA(path string) (*COLA, bool) {
    var cola = new(COLA)
    var err error
    cola.MetaFd, err = Open(path + "/meta", O_RDWR, S_IRALL | S_IWALL)
    if err != nil {
        return nil, false
    }
    Seek(cola.MetaFd, -8, os.SEEK_END)
    var buffer = make([]byte, 8)
    n, err := Read(cola.MetaFd, buffer)
    if err != nil || n != 8 {
        return nil, false
    }
    cola.Bitmap = bytesToUint64(buffer)
    var ok bool
    cola.blocks, ok = LoadBlocks(path + "/blx")
    if !ok {
        return nil, false
    }
    for i := 64; i > 0; i <<= 1 {
        if uint64(i) & cola.Bitmap > 0 {
            extpath := path + "/ext_" + strconv.Itoa(i)
            cola.extents[log(i)], ok = OpenExtent(extpath)
            if !ok {
                return nil, false
            }
        }
    }
    cola.LoadTime = time.Now().Unix()
    cola.Path = path
    // cola.PushDown
    return cola, true
}

func log(i int) int {
    var result int = 0
    for i >>= 7; i > 0; i >>= 1 {
        result++
    }
    return result
}
