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
    "bytes"
    "os/exec"
    . "syscall"
)

type Record struct {
    key    []byte
    value  []byte
}

type RecordSlice []Record

func (recs RecordSlice) Len() int {
    return len(recs)
}

func (recs RecordSlice) Less(i, j int) bool {
    return bytes.Compare(recs[i].key, recs[j].key) < 0
}

func (recs RecordSlice) Swap(i, j int) {
    temp   := recs[i]
    recs[i] = recs[j]
    recs[j] = temp
}

type Blocks struct {
    fd       int
    compact  bool
    bitmap   uint64
    records  RecordSlice
    path     string
}

func (blx *Blocks) Close() bool {
    return Close(blx.fd) == nil
}

func (blx *Blocks) Records() RecordSlice {
    return blx.records
}

func (blx *Blocks) Get(key []byte) []byte {
    for i := uint64(0); i < 64; i++ {
        if (uint64(1) << i) & blx.bitmap > 0 {
            if bytes.Compare(blx.records[i].key, key) == 0 {
                return blx.records[i].value
            }
        }
    }
    return nil
}

func (blx *Blocks) Set(key, value []byte) bool {
    for i := uint64(0); i < 64; i++ {
        if (uint64(1) << i) & blx.bitmap > 0 {
            if bytes.Compare(blx.records[i].key, key) == 0 {
                ok := writeRecord(blx.fd, i, key, value)
                if !ok {
                    return false
                }
                blx.records[i].value = make([]byte, len(value))
                copy(blx.records[i].value, value)
                return true
            }
        }
    }
    for i := uint64(0); i < 64; i++ {
        if (uint64(1) << i) & blx.bitmap == 0 {
            ok := writeRecord(blx.fd, i, key, value)
            if !ok {
                return false
            }
            blx.records[i].key = make([]byte, len(key))
            blx.records[i].value = make([]byte, len(value))
            copy(blx.records[i].key, key)
            copy(blx.records[i].value, value)
            blx.bitmap |= uint64(1) << i
            return true
        }
    }
    return false
}

func NewBlocks(path string) (*Blocks, bool) {
    var err error
    blx := new(Blocks)
    blx.path = path
    var mode int = O_RDWR | O_CREAT | O_TRUNC
    blx.fd, err = Open(blx.path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return blx, false
    }
    blx.records = make([]Record, 64)
    return blx, true
}

func OpenBlocks(path string) (*Blocks, int64) {
    var err error
    blx := new(Blocks)
    blx.path = path
    blx.fd, err = Open(blx.path, O_RDWR, S_IRALL | S_IWALL)
    if err != nil {
        return blx, -1
    }

    var trunc int64 = 0
    blx.records = make([]Record, 64)
    buffer := make([]byte, 8)
    /* Record format:
       | index  |    K    |    V    |   key   |  value  |
       |--------|---------|---------|-------- |---------|
       | 8 bits | 24 bits | 32 bits | K bytes | V bytes |
    */
    for {
        n, err := Read(blx.fd, buffer)
        if n == 0 {
            break
        }
        if err != nil || n != 8 {
            return blx, trunc
        }
        i, klen, vlen := decodeBlxHead(buffer)
        blx.records[i].key = make([]byte, klen)
        n, err = Read(blx.fd, blx.records[i].key)
        if err != nil || n != int(klen) {
            return blx, trunc
        }
        blx.records[i].value = make([]byte, vlen)
        n, err = Read(blx.fd, blx.records[i].value)
        if err != nil || n != int(vlen) {
            return blx, trunc
        }
        i = uint64(1) << i
        if blx.bitmap & i > 0 {
            blx.compact = true
        }
        blx.bitmap |= i
        trunc += 8 + int64(klen + vlen)
    }
    return blx, trunc
}

func WriteBlocks(blx *Blocks) bool {
    var path string = blx.path + ".1"
    var mode int = O_RDWR | O_CREAT | O_TRUNC
    fd, err := Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return false
    }
    var bitmap uint64 = blx.bitmap
    for i := uint64(0); i < 64; i++ {
        if bitmap & 1 == 1 {
            ok := writeRecord(fd, i, blx.records[i].key, blx.records[i].value)
            if !ok {
                return false
            }
        }
        bitmap >>= 1
    }
    Close(fd)
    cmd := exec.Command("cp", path, blx.path)
    err = cmd.Run()
    return err == nil
}

func LoadBlocks(path string) (*Blocks, bool) {
    blx, trunc := OpenBlocks(path)
    if trunc == -1 {
        return blx, false
    } else if blx.compact {
        Close(blx.fd)
        ok := WriteBlocks(blx)
        if !ok {
            return blx, false
        }
        blx, trunc = OpenBlocks(path)
        if trunc == -1 {
            return blx, false
        }
    }
    Ftruncate(blx.fd, trunc)
    return blx, true
}

func writeRecord(fd int, i uint64, key []byte, value []byte) bool {
    klen := uint64(len(key))
    vlen := uint64(len(value))
    head := encodeBlxHead(i, klen, vlen)
    n, err := Write(fd, head)
    if err != nil || n != 8 {
        return false
    }
    n, err = Write(fd, key)
    if err != nil || n != int(klen) {
        return false
    }
    n, err = Write(fd, value)
    if err != nil || n != int(vlen) {
        return false
    }
    return true
}

func decodeBlxHead(buffer []byte) (uint64, uint64, uint64) {
    idx   := uint64(buffer[0])
    klen  := uint64(buffer[1])
    klen   = klen << 8 + uint64(buffer[2])
    klen   = klen << 8 + uint64(buffer[3])
    vlen  := uint64(buffer[4])
    vlen   = vlen << 8 + uint64(buffer[5])
    vlen   = vlen << 8 + uint64(buffer[6])
    vlen   = vlen << 8 + uint64(buffer[7])
    return idx, klen, vlen
}

func encodeBlxHead(idx, klen, vlen uint64) []byte {
    var head [8]byte
    head[0] = byte(idx)
    head[1] = byte(klen >> 16)
    head[2] = byte(klen >> 8)
    head[3] = byte(klen)
    head[4] = byte(vlen >> 24)
    head[5] = byte(vlen >> 16)
    head[6] = byte(vlen >> 8)
    head[7] = byte(vlen)
    return head[:]
}
