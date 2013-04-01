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

type Block struct {
    /* E.g. number of blx_16.1 is (16 << 1 | 1) */
    number  uint8
    raw     []byte
    key     []byte
    value   []byte
    path    string
}

func (block *Block) Free() bool {
    return Munmap(block.raw) == nil && Unlink(block.path) == nil
}

func OpenBlock(dir string, number uint8) (*Block, bool) {
    block := new(Block)
    var path string = blockPath(dir, number)
    fd, err := Open(path, O_RDONLY, S_IREAD)
    if err != nil {
        return block, false
    }

    /* Decode block head. */
    buffer := make([]byte, 8)
    n, err := Read(fd, buffer[:2])
    if err != nil || n != 2 {
        return block, false
    }
    var hlen, klen, vlen int
    if buffer[0] == 255 {
        n, err = Read(fd, buffer[2:8])
        if err != nil || n != 6 {
            return block, false
        }
        hlen = 8
        for i := 1; i <= 3; i++ {
            klen = klen << 8 + int(buffer[i])
        }
        for i := 4; i <= 7; i++ {
            vlen = vlen << 8 + int(buffer[i])
        }
    } else {
        hlen = 2
        klen = int(buffer[0])
        vlen = int(buffer[1])
    }

    /* New a Block struct. */
    var length int = hlen + klen + vlen
    raw, err := Mmap(fd, 0, length, PROT_READ, MAP_PRIVATE)
    Close(fd)
    if err != nil {
        return block, false
    }
    *block = Block {
        number : number,
        raw    : raw,
        key    : raw[hlen : hlen + klen],
        value  : raw[hlen + klen : length],
        path   : path,
    }
    return block, true
}

func UpdateBlock(dir string, number uint8, key []byte, value []byte) (*Block, bool) {
    block := new(Block)
    if len(key) > int(MAX_KEY_LEN) || len(value) > int(MAX_VALUE_LEN) {
        return block, false
    }

    /* Create a file. */
    var path string = blockPath(dir, number)
    var mode int = O_RDWR | O_CREAT | O_TRUNC
    var S_IRALL uint32 = S_IRUSR | S_IRGRP | S_IROTH
    var S_IWALL uint32 = S_IWUSR | S_IWGRP | S_IWOTH
    fd, err := Open(path, mode, S_IRALL | S_IWALL)
    if err != nil {
        return block, false
    }

    /* Write block head, key and value. */
    head := blockHead(len(key), len(value))
    _, err = Write(fd, head)
    if err != nil {
        return block, false
    }
    _, err = Write(fd, key)
    if err != nil {
        return block, false
    }
    _, err = Write(fd, value)
    if err != nil {
        return block, false
    }

    /* New a Block struct with mmap. */
    length := len(head) + len(key) + len(value)
    raw, err := Mmap(fd, 0, length, PROT_READ, MAP_PRIVATE)
    Close(fd)
    if err != nil {
        return block, false
    }
    *block = Block {
        number : number,
        raw    : raw,
        key    : raw[len(head) : len(head) + len(key)],
        value  : raw[len(head) + len(key) : length],
        path   : path,
    }
    return block, true
}

func blockPath(dir string, number uint8) string {
    var suffix string
    if number & 1 == 1 {
        suffix = strconv.Itoa(int(number >> 1)) + ".1"
    } else {
        suffix = strconv.Itoa(int(number >> 1)) + ".0"
    }
    return dir + "/blx_" + suffix
}

func blockHead(klen, vlen int) []byte {
    var head []byte
    /* Block head is either 2 bytes or 8 bytes. */
    if klen < 255 && vlen < 255 {
        head = make([]byte, 2)
        head[0] = byte(klen)
        head[1] = byte(vlen)
    } else {
        head = make([]byte, 8)
        head[0] = 255
        head[1] = byte(klen >> 16)
        head[2] = byte(klen >> 8)
        head[3] = byte(klen)
        head[4] = byte(vlen >> 24)
        head[5] = byte(vlen >> 16)
        head[6] = byte(vlen >> 8)
        head[7] = byte(vlen)
    }
    return head
}
