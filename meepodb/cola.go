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
    extents   [64]*Extent
    Path      string
}

func (cola *COLA) Get(key []byte) []byte {
    var value []byte
    /* Try to get from blocks */
    value = cola.blocks.Get(key)
    if value != nil {
        return value
    }
    /* Get from extents */
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
    var ok bool = cola.blocks.Set(key, value)
    if !ok {
        return false
    }
    if cola.blocks.count == MAX_RECORDS {
        return cola.PushDown()
    }
    return true
}

func (cola *COLA) PushDown() bool {
    var ext *Extent = BlocksToMemExtent(cola.blocks.records)
    var i int
    for i = int(MAX_RECORDS); i > 0; i <<= 1 {
        /* If current extent does not exist... */
        if uint64(i) & cola.Bitmap == 0 {
            if uint64(i) > cola.Bitmap {
                CompactMemExtent(ext)
            }
            cola.Bitmap |= uint64(i)
            break
        }
        /* Merge current extent and pushed-down extent */
        ext = MergeMemExtents(cola.extents[log(i)], ext)
        cola.extents[log(i)].Free()
        /* If reach the bottom... */
        if cola.Bitmap & ^uint64(i) < uint64(i) {
            CompactMemExtent(ext)
        }
        /* If merged size can fit current extent... */
        if ext.total <= uint64(i) {
            break
        }
        cola.Bitmap &= ^uint64(i)
    }
    /* Write the new extent to disk */
    var path string = cola.Path + "/ext_" + strconv.Itoa(i)
    var mode int = O_WRONLY | O_CREAT | O_TRUNC
    fd, err := Open(path + ".1", mode, S_IRALL | S_IWALL)
    if err != nil {
        return false
    }
    n, err := Write(fd, ext.raw)
    if err != nil || n != int(ext.size) {
        return false
    }
    Close(fd)
    err = Rename(path + ".1", path)
    if err != nil {
        return false
    }
    /* Load the new extent to memory */
    var ok bool
    cola.extents[log(i)], ok = OpenExtent(path)
    if !ok {
        return false
    }
    /* Update the bitmap on disk */
    n, err = Write(cola.MetaFd, uint64ToBytes(cola.Bitmap))
    if err != nil || n != 8 {
        return false
    }
    /* Flush blocks in memory or blx file on disk when it gets large */
    offset, err := Seek(cola.blocks.fd, 0, os.SEEK_CUR)
    if err != nil {
        return false
    }
    if offset < BLX_BUF_SIZE {
        cola.blocks.count = 0
        cola.blocks.dict = make(map[string]uint64, MAX_RECORDS)
        return true
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
    /* meta */
    var mode int = O_WRONLY | O_CREAT
    cola.MetaFd, err = Open(path + "/meta", mode, S_IRALL | S_IWALL)
    if err != nil {
        return nil, false
    }
    n, err := Write(cola.MetaFd, uint64ToBytes(cola.Bitmap))
    if err != nil || n != 8 {
        return nil, false
    }
    /* blx */
    var ok bool
    cola.blocks, ok = NewBlocks(path + "/blx")
    if !ok {
        return nil, false
    }
    cola.Path = path
    return cola, true
}

func OpenCOLA(path string) (*COLA, bool) {
    var err error
    var cola = new(COLA)
    cola.MetaFd, err = Open(path + "/meta", O_RDWR, S_IRALL | S_IWALL)
    if err != nil {
        return nil, false
    }
    /* Read last bitmap */
    Seek(cola.MetaFd, -8, os.SEEK_END)
    var buffer = make([]byte, 8)
    n, err := Read(cola.MetaFd, buffer)
    if err != nil || n != 8 {
        return nil, false
    }
    cola.Bitmap = bytesToUint64(buffer)
    /* Delete all the old bitmaps */
    Seek(cola.MetaFd, 0, os.SEEK_SET)
    n, err = Write(cola.MetaFd, buffer)
    if err != nil || n != 8 {
        return nil, false
    }
    Ftruncate(cola.MetaFd, 8)
    /* blx */
    var ok bool
    cola.blocks, ok = LoadBlocks(path + "/blx")
    if !ok {
        return nil, false
    }
    /* Extents */
    for i := 64; i > 0; i <<= 1 {
        if uint64(i) & cola.Bitmap > 0 {
            extpath := path + "/ext_" + strconv.Itoa(i)
            cola.extents[log(i)], ok = OpenExtent(extpath)
            if !ok {
                return nil, false
            }
        }
    }
    cola.Path = path
    if cola.blocks.count == MAX_RECORDS {
        ok = cola.PushDown()
    }
    return cola, ok
}

func log(i int) int {
    var result int = 0
    for i /= int(MAX_RECORDS) * 2; i > 0; i >>= 1 {
        result++
    }
    return result
}
