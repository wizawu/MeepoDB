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
    "os"
    "sort"
    "strconv"
    . "syscall"
)

type Extent struct {
    /* E.g. number of ext_128.1 is (128 << 1 | 1). */
    number   uint64
    raw      []byte
    /* Size of extent */
    size     uint64
    /* Number of records */
    total    uint64
    index    []byte
    path     string
}

func (extent *Extent) Index(i uint64) (uint64, uint64) {
    var entry  uint64 = bytesToUint64(extent.index[i * 8 : i * 8 + 8])
    var offset uint64 = entry >> KEY_BITS
    var klen   uint64 = entry & MAX_KEY_LEN
    return offset, klen
}

func (extent *Extent) Key(i uint64) []byte {
    offset, klen := extent.Index(i)
    return extent.raw[offset : offset + klen]
}

func (extent *Extent) Record(i uint64) ([]byte, []byte) {
    offset, klen := extent.Index(i)
    var end uint64
    if i == extent.total - 1 {
        end = extent.size
    } else {
        end, _ = extent.Index(i + 1)
    }
    return extent.raw[offset : offset + klen],
           extent.raw[offset + klen : end]
}

/* Binary search */
func (extent *Extent) Find(key []byte) int64 {
    var result int64 = -1
    var left, right uint64 = 0, extent.total - 1
    for left < right {
        var middle uint64 = (left + right) / 2
        var midkey []byte = extent.Key(middle)
        var flag int = bytes.Compare(midkey, key)
        if flag == 0 {
            result = int64(middle)
            break
        } else if flag < 0 {
            left = middle + 1
        } else {
            right = middle
        }
    }
    return result
}

func (extent *Extent) Free() bool {
    return Munmap(extent.raw) == nil && Unlink(extent.path) == nil
}

func OpenExtent(dir string, number uint64) (*Extent, bool) {
    extent := new(Extent)
    var path string = pathname(dir, number)
    fd, err := Open(path, O_RDONLY, S_IREAD)
    if err != nil {
        return extent, false
    }

    /* Decode extent head */
    buffer := make([]byte, 16)
    n, err := Read(fd, buffer)
    if err != nil || n != 16 {
        return extent, false
    }
    var size  uint64 = bytesToUint64(buffer[0 : 8])
    var total uint64 = bytesToUint64(buffer[8 : 16])

    /* Extent struct */
    raw, err := Mmap(fd, 0, size, PROT_READ, MAP_PRIVATE)
    Close(fd)
    if err != nil {
        return extent, false
    }
    var index []byte = raw[16 : 16 + 8 * total]
    *extent = Extent {
        number  : number,
        raw     : raw,
        size    : size,
        total   : total,
        index   : index,
        path    : path,
    }
    return extent, true
}

func BlocksToExtent(dir string, number uint64, blocks BlockSlice) (*Extent, bool) {
    sort.Sort(blocks)
    extent := new(Extent)

    /* Open file */
    var path string = pathname(dir, int(number))
    var mode int = O_RDWR | O_CREAT | O_TRUNC
    var S_IRALL uint32 = S_IRUSR | S_IRGRP | S_IROTH
    var S_IWALL uint32 = S_IWUSR | S_IWGRP | S_IWOTH
    fd, err := Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return extent, false
    }

    /* Extent format:
       |  size   |  total  |  index   | records |
       |---------|---------|----------|---------|
       | 8 bytes | 8 bytes | 8X bytes | Y bytes |
    */
    var total uint64 = 64
    var size uint64 = 16 + 8 * total
    /* Calculate offsets */
    var offsets [total]uint64
    for i := 0; i < total; i++ {
        offsets[i] = size
        size += uint64(len(blocks[i].key) + len(blocks[i].value))
    }
    /* Write extent head */
    n, err := Write(fd, uint64ToBytes(size))
    if err != nil || n != 8 {
        return extent, false
    }
    n, err = Write(fd, uint64ToBytes(total))
    if err != nil || n != 8 {
        return extent, false
    }
    /* Write index */
    for i := 0; i < total; i++ {
        ent := offsets[i] << KEY_BITS + uint64(len(blocks[i].key))
        n, err = Write(fd, uint64ToBytes(ent))
        if err != nil || n != 8 {
            return extent, false
        }
    }
    /* Write records */
    for i := 0; i < total; i++ {
        n, err = Write(fd, blocks[i].key)
        if err != nil || n != len(blocks[i].key) {
            return extent, false
        }
        n, err = Write(fd, blocks[i].value)
        if err != nil || n != len(blocks[i].value) {
            return extent, false
        }
    }

    /* Extent struct */
    raw, err := Mmap(fd, 0, size, PROT_READ, MAP_PRIVATE)
    Close(fd)
    if err != nil {
        return extent, false
    }
    *extent = Extent {
        number  : number,
        raw     : raw,
        size    : size,
        total   : total,
        index   : raw[16 : 16 + 8 * total],
        path    : path,
    }
    return extent, true
}

func MergeExtents(dir string, number uint64, ext [2]*Extent) (*Extent, bool) {
    extent := new(Extent)
    /* Open file */
    var path string = pathname(dir, int(number))
    var mode int = O_RDWR | O_CREAT | O_TRUNC
    var S_IRALL uint32 = S_IRUSR | S_IRGRP | S_IROTH
    var S_IWALL uint32 = S_IWUSR | S_IWGRP | S_IWOTH
    fd, err := Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return extent, false
    }

    /* Get total and entries */
    var total    uint64
    var size     uint64 = 16
    var entries  [number >> 1]uint64
    var flags    [number >> 1]int
    var k, v     []byte
    var iter     [2]uint64
    len_ := [2]uint64{ ext[0].total, ext[1].total }
    for iter[0] < len_[0] && iter[1] < len_[1] {
        flag := bytes.Compare(ext[0].Key(iter[0]), ext[1].Key(iter[1]))
        if flag == 0 {
            k, v = ext[1].Record(iter[1])
            iter[0], iter[1] = iter[0] + 1, iter[1] + 1
        } else if flag < 0 {
            k, v := ext[0].Record(iter[0])
            iter[0]++
        } else {
            k, v = ext[1].Record(iter[1])
            iter[1]++
        }
        /* Here offset is not the eventual one because index length has not been
           added to it. */
        entries[total] = size << KEY_BITS + uint64(len(k))
        flags[total] = flag
        size += uint64(len(k) + len(v))
        total++
    }
    for x := 0; x <= 1; x++ {
        for ; iter[x] < len_[x]; iter[x]++ {
            k, v = ext[x].Record(iter[x])
            entries[total] = size << KEY_BITS + uint64(len(k))
            /* If x = 0, flag < 0; x = 1, flag > 0. */
            flags[total] = x * 2 - 1
            size += uint64(len(k) + len(v))
            total++
        }
    }
    size += 8 * total
    /* Write extent head */
    n, err := Write(fd, uint64ToBytes(size))
    if err != nil || n != 8 {
        return extent, false
    }
    n, err = Write(fd, uint64ToBytes(total))
    if err != nil || n != 8 {
        return extent, false
    }
    /* Write index */
    for i := uint64(0); i < total; i++ {
        n, err = Write(fd, uint64ToBytes(entries[i] + 8 * total << KEY_BITS))
        if err != nil || n != 8 {
            return extent, false
        }
    }
    /* Write records */
    iter[0], iter[1] = 0, 0
    for i := uint64(0); i < total; i++ {
        if flags[i] == 0 {
            k, v = ext[1].Record(iter[1])
            iter[0], iter[1] = iter[0] + 1, iter[1] + 1
        }  else if flags[i] < 0 {
            k, v = ext[0].Record(iter[0])
            iter[0]++
        } else {
            k, v = ext[1].Record(iter[1])
            iter[1]++
        }
        n, err = Write(fd, k)
        if err != nil || n != len(k) {
            return extent, false
        }
        n, err = Write(fd, v)
        if err != nil || n != len(v) {
            return extent, false
        }
    }

    /* Extent struct */
    raw, err := Mmap(fd, 0, size, PROT_READ, MAP_PRIVATE)
    Close(fd)
    if err != nil {
        return extent, false
    }
    *extent = Extent {
        number  : number,
        raw     : raw,
        size    : size,
        total   : total,
        index   : raw[16 : 16 + 8 * total],
        path    : path,
    }
    return extent, true
}

func pathname(dir string, number int) string {
    var suffix string
    if number & 1 == 1 {
        suffix = strconv.Itoa(number >> 1) + ".1"
    } else {
        suffix = strconv.Itoa(number >> 1) + ".0"
    }
    return dir + "/ext_" + suffix
}
