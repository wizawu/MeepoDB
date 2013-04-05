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
    count    uint64
    records  RecordSlice
    path     string
    dict     map[string]uint64
}

func (blx *Blocks) Close() bool {
    return Close(blx.fd) == nil
}

func (blx *Blocks) Records() RecordSlice {
    return blx.records
}

func (blx *Blocks) Get(key []byte) []byte {
    i, ok := blx.dict[string(key)]
    if ok {
        return blx.records[i].value
    }
    return nil
}

func (blx *Blocks) Set(key, value []byte) bool {
    i, ok := blx.dict[string(key)]
    if ok {
        ok = writeRecord(blx.fd, i, key, value)
        if !ok {
            return false
        }
        blx.records[i].value = make([]byte, len(value))
        copy(blx.records[i].value, value)
        return true
    }
    if blx.count < MAX_RECORDS {
        i = blx.count
        ok = writeRecord(blx.fd, i, key, value)
        if !ok {
            return false
        }
        blx.records[i].key = make([]byte, len(key))
        blx.records[i].value = make([]byte, len(value))
        copy(blx.records[i].key, key)
        copy(blx.records[i].value, value)
        blx.dict[string(key)] = i
        blx.count++
        return true
    }
    return false
}

func NewBlocks(path string) (*Blocks, bool) {
    var err error
    var blx = new(Blocks)
    var mode int = O_WRONLY | O_CREAT | O_TRUNC
    blx.fd, err = Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return nil, false
    }
    blx.path = path
    blx.records = make([]Record, MAX_RECORDS)
    blx.dict = make(map[string]uint64, MAX_RECORDS)
    return blx, true
}

func OpenBlocks(path string) (*Blocks, int64) {
    var err error
    var blx = new(Blocks)
    var mode int = O_RDWR | O_APPEND
    blx.fd, err = Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return blx, -1
    }

    blx.path = path
    blx.records = make([]Record, MAX_RECORDS)
    blx.dict = make(map[string]uint64, MAX_RECORDS)
    var trunc int64 = 0
    var buffer = make([]byte, 8)
    /* Record format:
       | index  |    K    |    V    |   key   |  value  |
       |--------|---------|---------|-------- |---------|
       | 8 bits | 24 bits | 32 bits | K bytes | V bytes |
    */
    for {
        /* Read record head */
        n, err := Read(blx.fd, buffer)
        if n == 0 {
            break
        }
        if err != nil || n != 8 {
            return blx, trunc
        }
        i, klen, vlen := decodeBlxHead(buffer)
        /* Read key */
        blx.records[i].key = make([]byte, klen)
        n, err = Read(blx.fd, blx.records[i].key)
        if err != nil || n != int(klen) {
            return blx, trunc
        }
        /* Read value */
        blx.records[i].value = make([]byte, vlen)
        n, err = Read(blx.fd, blx.records[i].value)
        if err != nil || n != int(vlen) {
            return blx, trunc
        }
        /* Check whether there are more than one records of the key */
        _, ok := blx.dict[string(blx.records[i].key)]
        if ok {
            blx.compact = true
        }

        blx.dict[string(blx.records[i].key)] = i
        if i + 1 > blx.count {
            blx.count = i + 1
        }
        trunc += 8 + int64(klen + vlen)
    }
    return blx, trunc
}

func WriteBlocks(blx *Blocks) bool {
    var mode int = O_WRONLY | O_CREAT | O_TRUNC
    fd, err := Open(blx.path + ".1", mode, S_IRALL | S_IWALL)
    if err != nil {
        return false
    }
    for i := uint64(0); i < blx.count; i++ {
        ok := writeRecord(fd, i, blx.records[i].key, blx.records[i].value)
        if !ok {
            return false
        }
    }
    Close(fd)
    err = Rename(blx.path + ".1", blx.path)
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
    idx   := uint64(buffer[0]) << 4 + uint64(buffer[1]) >> 4
    klen  := uint64(buffer[1]) & 15
    klen   = klen << 8 + uint64(buffer[2])
    klen   = klen << 8 + uint64(buffer[3])
    vlen  := uint64(buffer[4])
    vlen   = vlen << 8 + uint64(buffer[5])
    vlen   = vlen << 8 + uint64(buffer[6])
    vlen   = vlen << 8 + uint64(buffer[7])
    return idx, klen, vlen
}

func encodeBlxHead(idx, klen, vlen uint64) []byte {
    return Uint64ToBytes(idx << 52 + klen << 32 + vlen)
}
