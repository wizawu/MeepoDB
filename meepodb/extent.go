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
    records  []byte
    path     string
}

func (extent *Extent) Index(i uint64) (uint64, uint64) {
    var entry  uint64 = bytesToUint64(extent.index[i*8 : i*8+8])
    var offset uint64 = entry >> KEY_BITS
    var klen   uint64 = entry & MAX_KEY_LEN
    return offset, klen
}

func (extent *Extent) Key(i uint64) []byte {
    offset, klen := extent.Index(i)
    return extent.records[offset : offset+klen]
}

func (extent *Extent) Record(i uint64) ([]byte, []byte) {
    offset, klen := extent.Index(i)
    var end uint64
    if i == extent.total - 1 {
        end = extent.size
    } else {
        end, _ = extent.Index(i + 1)
    }
    return extent.records[offset : offset+klen],
           extent.records[offset+klen : end]
}

/* Binary search */
func (extent *Extent) Find(key []byte) int64 {
    var result int64 = -1
    var left, right uint64 = 0, extent.total - 1
    for left < right {
        var middle uint64 = (left + right) / 2
        var midkey []byte = extent.Key(middle)
        var flag int = bytesCompare(midkey, key)
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
    return Munmap(extent.raw) == nil && Unlink(extent.block) == nil
}

func OpenExtent(dir string, number uint64) (*Extent, bool) {
    extent := new(Extent)
    var path string = pathname(dir, false, number)
    fd, err := Open(path, O_RDONLY, S_IREAD)
    if err != nil {
        return extent, false
    }

    /* Decode extent head. */
    buffer := make([]byte, 16)
    n, err := Read(fd, buffer)
    if err != nil || n != 16 {
        return extent, false
    }
    var size  uint64 = bytesToUint64(buffer[0:8])
    var total uint64 = bytesToUint64(buffer[8:16])

    /* New a Block struct. */
    raw, err := Mmap(fd, 0, size, PROT_READ, MAP_PRIVATE)
    Close(fd)
    if err != nil {
        return extent, false
    }
    var index []byte = raw[16 : total*8+16]
    var records []byte = raw[total*8+16 : size]
    *extent = Extent {
        number: number,
        raw: raw,
        size: size,
        total: total,
        index: index,
        records: records,
        path: path,
    }
    return extent, true
}

func BlocksToExtent(blocks []Block, flag bool) (*Extent, bool) {
}

func MergeExtents(ext0, ext1 *Extent, flag bool) (*Extent, bool) {
}

func bytesToUint64(bytes []byte) uint64 {
    var x uint64
    for _, b := range bytes[:8] {
        x = x << 8 + uint64(b)
    }
    return x
}

/* if a > b  return positive integer
   if a = b  return 0
   if a < b  return negative integer 
*/
func bytesCompare(a []byte, b []byte) int {
    var clen int = len(a)
    if len(b) < clen {
        clen = len(b)
    }
    for i := 0; i < clen; i++ {
        if a[i] < b[i] {
            return -1
        } else if a[i] > b[i] {
            return 1
        }
    }
    return len(a) - len(b)
}
