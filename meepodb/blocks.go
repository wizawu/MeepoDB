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
    "sort"
    "strconv"
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

func OpenBlocks(dir string) (*Blocks, int64) {
    blx := new(Blocks)
    blx.path = dir + "/blx"
    var S_IRALL uint32 = S_IRUSR | S_IRGRP | S_IROTH
    var S_IWALL uint32 = S_IWUSR | S_IWGRP | S_IWOTH
    blx.fd, err := Open(blx.path, O_RDWR, S_IRALL | S_IWALL)
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
        if err != nil || n != klen {
            return blx, trunc
        }
        blx.records[i].value = make([]byte, vlen)
        n, err = Read(blx.fd, blx.records[i].value)
        if err != nil || n != vlen {
            return blx, trunc
        }
        i = uint64(1) << i
        if blx.bitmap & i > 0 {
            blx.compact = true
        }
        blx.bitmap |= i
        trunc += 8 + klen + vlen
    }
    return blx, trunc
}

func WriteBlocks(blx *Blocks) bool {
    var path string = blx.path + ".1"
    var mode int = O_RDWR | O_CREAT | O_TRUNC
    var S_IRALL uint32 = S_IRUSR | S_IRGRP | S_IROTH
    var S_IWALL uint32 = S_IWUSR | S_IWGRP | S_IWOTH
    fd, err := Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return false
    }
    var bitmap uint64 = blx.bitmap
    for i := uint64(0); i < 64; i++ {
        if bitmap & 1 == 1 {
            klen := uint64(len(blx.records[i].key))
            vlen := uint64(len(blx.records[i].value))
            head := EncodeBlxHead(i, klen, vlen)
            n, err := Write(fd, head)
            if err != nil || n != 8 {
                return false
            }
            n, err = Write(fd, blx.records[i].key)
            if err != nil || n != klen {
                return false
            }
            n, err = Write(fd, blx.records[i].value)
            if err != nil || n != vlen {
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

func LoadBlocks(dir string) (*Blocks, bool) {
    blx, trunc := OpenBlocks(dir)
    if trunc == -1 {
        return blx, false
    } else if blx.compact {
        Close(blx.fd)
        ok := WriteBlocks(blx)
        if !ok {
            return blx, false
        }
        blx, trunc = OpenBlocks(dir)
        if trunc == -1 {
            return blx, false
        }
    }
    Ftruncate(blx.fd, trunc)
    return blx, true
}

func decodeBlxHead(buffer []byte) uint64, uint64, uint64 {
    idx   := uint64(buffer[0])
    klen  := uint64(buffer[1])
    klen   = klen << 8 + uint64(buffer[2])
    klen   = klen << 8 + uint64(buffer[3])
    vlen  := uint64(buffer[4])
    vlen   = vlen << 8 + uint64(buffer[5])
    vlen   = vlen << 8 + uint64(buffer[6])
    vlen   = vlen << 8 + uint64(buffer[7])
    return idx, klen, vlen
)

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
}
