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
    "sort"
    . "syscall"
)

type WriteBuf struct {
    buffer  []byte
    offset  int
}

func (wbuf *WriteBuf) Write(data []byte) {
    copy(wbuf.buffer[wbuf.offset:], data)
    wbuf.offset += len(data)
}

func (wbuf *WriteBuf) ReadAll() []byte {
    return wbuf.buffer
}

func NewWriteBuf(size int) *WriteBuf {
    var wbuf = new(WriteBuf)
    wbuf.buffer = make([]byte, size)
    return wbuf
}

type Extent struct {
    raw      []byte
    /* Size of extent */
    size     uint64
    /* Number of records */
    total    uint64
    index    []byte
    path     string
}

func (extent *Extent) Count() uint64 {
    return extent.total
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
    var left, right uint64 = 0, extent.total
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
    return Munmap(extent.raw) == nil
}

func OpenExtent(path string) (*Extent, bool) {
    /* Extent format:
       |  size   |  total  |  index   | records |
       |---------|---------|----------|---------|
       | 8 bytes | 8 bytes | 8X bytes | Y bytes |
    */
    extent := new(Extent)
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
    raw, err := Mmap(fd, 0, int(size), PROT_READ, MAP_PRIVATE)
    Close(fd)
    if err != nil {
        return extent, false
    }
    var index []byte = raw[16 : 16 + 8 * total]
    *extent = Extent {
        raw     : raw,
        size    : size,
        total   : total,
        index   : index,
        path    : path,
    }
    return extent, true
}

func OpenMemExtent(buffer []byte) *Extent {
    var size  uint64 = bytesToUint64(buffer[0 : 8])
    var total uint64 = bytesToUint64(buffer[8 : 16])
    return &Extent {
        raw   : buffer,
        size  : size,
        total : total,
        index : buffer[16 : 16 + 8 * total],
    }
}

func BlocksToMemExtent(records RecordSlice) *Extent {
    sort.Sort(records)
    var total uint64 = MAX_RECORDS
    var size uint64 = 16 + 8 * total
    var offsets = make([]uint64, total)
    for i := uint64(0); i < total; i++ {
        offsets[i] = size
        size += uint64(len(records[i].key) + len(records[i].value))
    }

    /* Write to WriteBuf */
    var wbuf = NewWriteBuf(int(size))
    wbuf.Write(uint64ToBytes(size))
    wbuf.Write(uint64ToBytes(total))
    for i := uint64(0); i < total; i++ {
        ent := offsets[i] << KEY_BITS + uint64(len(records[i].key))
        wbuf.Write(uint64ToBytes(ent))
    }
    for i := uint64(0); i < total; i++ {
        wbuf.Write(records[i].key)
        wbuf.Write(records[i].value)
    }
    return OpenMemExtent(wbuf.ReadAll())
}

func MergeMemExtents(ext0, ext1 *Extent) *Extent {
    /* Get total and entries */
    ext := [2]*Extent{ ext0, ext1 }
    var total  uint64
    var size   uint64 = 16
    var limit  uint64 = ext[0].total + ext[1].total
    var k, v   []byte
    var iter   [2]uint64
    entries := make([]uint64, limit)
    flags   := make([]int, limit)
    len_    := [2]uint64{ ext[0].total, ext[1].total }
    for iter[0] < len_[0] && iter[1] < len_[1] {
        flag := bytes.Compare(ext[0].Key(iter[0]), ext[1].Key(iter[1]))
        if flag == 0 {
            k, v = ext[1].Record(iter[1])
            iter[0], iter[1] = iter[0] + 1, iter[1] + 1
        } else if flag < 0 {
            k, v = ext[0].Record(iter[0])
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

    /* Write to WriteBuf */
    var wbuf = NewWriteBuf(int(size))
    wbuf.Write(uint64ToBytes(size))
    wbuf.Write(uint64ToBytes(total))
    for i := uint64(0); i < total; i++ {
        wbuf.Write(uint64ToBytes(entries[i] + 8 * total << KEY_BITS))
    }
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
        wbuf.Write(k)
        wbuf.Write(v)
    }
    return OpenMemExtent(wbuf.ReadAll())
}

func CompactMemExtent(ext *Extent) {
    var size uint64 = 16
    var total uint64
    var entries = make([]uint64, ext.total)
    for i := uint64(0); i < ext.total; i++ {
        k, v := ext.Record(i)
        if len(v) > 0 {
            entries[total] = size << KEY_BITS + uint64(len(k))
            size += uint64(len(k) + len(v))
            total++
        }
    }
    if total == ext.total {
        return
    }
    size += 8 * total

    /* Write to WriteBuf */
    var wbuf = NewWriteBuf(int(size))
    wbuf.Write(uint64ToBytes(size))
    wbuf.Write(uint64ToBytes(total))
    for i := uint64(0); i < total; i++ {
        wbuf.Write(uint64ToBytes(entries[i] + 8 * total << KEY_BITS))
    }
    for i := uint64(0); i < ext.total; i++ {
        k, v := ext.Record(i)
        if len(v) > 0 {
            wbuf.Write(k)
            wbuf.Write(v)
        }
    }
    /* Update ext */
    ext.raw   = wbuf.ReadAll()
    ext.size  = size
    ext.total = total
    ext.index = ext.raw[16 : 16 + 8 * total]
}

/* Too slow */
func BlocksToExtent2(path string, records RecordSlice) bool {
    sort.Sort(records)
    var total uint64 = 64
    var size uint64 = 16 + 8 * total
    offsets := make([]uint64, total)
    for i := uint64(0); i < total; i++ {
        offsets[i] = size
        size += uint64(len(records[i].key) + len(records[i].value))
    }

    var mode int = O_WRONLY | O_CREAT | O_TRUNC
    fd, err := Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return false
    }
    /* Write extent head */
    n, err := Write(fd, uint64ToBytes(size))
    if err != nil || n != 8 {
        return false
    }
    n, err = Write(fd, uint64ToBytes(total))
    if err != nil || n != 8 {
        return false
    }
    /* Write index */
    for i := uint64(0); i < total; i++ {
        ent := offsets[i] << KEY_BITS + uint64(len(records[i].key))
        n, err = Write(fd, uint64ToBytes(ent))
        if err != nil || n != 8 {
            return false
        }
    }
    /* Write records */
    for i := uint64(0); i < total; i++ {
        n, err = Write(fd, records[i].key)
        if err != nil || n != len(records[i].key) {
            return false
        }
        n, err = Write(fd, records[i].value)
        if err != nil || n != len(records[i].value) {
            return false
        }
    }
    Close(fd)
    return true
}

/* Too slow */
func MergeExtents2(path string, ext0, ext1 *Extent) bool {
    /* Get total and entries */
    ext := [2]*Extent{ ext0, ext1 }
    var total  uint64
    var size   uint64 = 16
    var limit  uint64 = ext[0].total + ext[1].total
    var k, v   []byte
    var iter   [2]uint64
    entries := make([]uint64, limit)
    flags   := make([]int, limit)
    len_    := [2]uint64{ ext[0].total, ext[1].total }
    for iter[0] < len_[0] && iter[1] < len_[1] {
        flag := bytes.Compare(ext[0].Key(iter[0]), ext[1].Key(iter[1]))
        if flag == 0 {
            k, v = ext[1].Record(iter[1])
            iter[0], iter[1] = iter[0] + 1, iter[1] + 1
        } else if flag < 0 {
            k, v = ext[0].Record(iter[0])
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

    var mode int = O_WRONLY | O_CREAT | O_TRUNC
    fd, err := Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return false
    }
    /* Write extent head */
    n, err := Write(fd, uint64ToBytes(size))
    if err != nil || n != 8 {
        return false
    }
    n, err = Write(fd, uint64ToBytes(total))
    if err != nil || n != 8 {
        return false
    }
    /* Write index */
    for i := uint64(0); i < total; i++ {
        n, err = Write(fd, uint64ToBytes(entries[i] + 8 * total << KEY_BITS))
        if err != nil || n != 8 {
            return false
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
            return false
        }
        n, err = Write(fd, v)
        if err != nil || n != len(v) {
            return false
        }
    }
    Close(fd)
    return true
}

/* Too slow */
func CompactExtent2(path string) bool {
    ext, ok := OpenExtent(path)
    if !ok {
        return false
    }
    var size uint64 = 16
    var total uint64
    var entries = make([]uint64, ext.total)
    for i := uint64(0); i < ext.total; i++ {
        k, v := ext.Record(i)
        if len(v) > 0 {
            entries[total] = size << KEY_BITS + uint64(len(k))
            size += uint64(len(k) + len(v))
            total++
        }
    }
    if total == ext.total {
        return true
    }
    var mode int = O_WRONLY | O_CREAT | O_TRUNC
    fd, err := Open(path + ".c", mode, S_IRALL | S_IWALL)
    if err != nil {
        return false
    }
    /* Write extent head */
    size += 8 * total
    n, err := Write(fd, uint64ToBytes(size))
    if err != nil || n != 8 {
        return false
    }
    n, err = Write(fd, uint64ToBytes(total))
    if err != nil || n != 8 {
        return false
    }
    /* Write index */
    for i := uint64(0); i < total; i++ {
        n, err = Write(fd, uint64ToBytes(entries[i] + 8 * total << KEY_BITS))
        if err != nil || n != 8 {
            return false
        }
    }
    /* Write records */
    for i := uint64(0); i < ext.total; i++ {
        k, v := ext.Record(i)
        if len(v) > 0 {
            n, err = Write(fd, k)
            if err != nil || n != len(k) {
                return false
            }
            n, err = Write(fd, v)
            if err != nil || n != len(v) {
                return false
            }
        }
    }
    Close(fd)
    ext.Free()
    err = Rename(path + ".c", path)
    return err == nil
}
