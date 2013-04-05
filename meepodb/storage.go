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
    . "syscall"
)

type Storage struct {
    colas map[string](*COLA)
}

func NewStorage() *Storage {
    var strg = new(Storage)
    strg.colas = make(map[string](*COLA), 16)
    return strg
}

func (strg *Storage) COLA(name []byte) *COLA {
    var str = string(name)
    cola, ok := strg.colas[str]
    if !ok {
        cola, ok = OpenCOLA(DB_DIR + "/" + str)
        if !ok {
            cola, ok = NewCOLA(DB_DIR + "/" + str)
            if !ok {
                return nil
            }
        }
        strg.colas[str] = cola
    }
    return cola
}

func (strg *Storage) ExistentCOLA(name []byte) *COLA {
    var str = string(name)
    cola, ok := strg.colas[str]
    if !ok {
        cola, ok = OpenCOLA(DB_DIR + "/" + str)
        if !ok {
            return nil
        }
        strg.colas[str] = cola
    }
    return cola
}

func (strg *Storage) Get(table, key []byte) []byte {
    var cola = strg.ExistentCOLA(table)
    if cola == nil {
        return nil
    }
    return cola.Get(key)
}

func (strg *Storage) Set(table, key, value []byte) bool {
    var cola = strg.COLA(table)
    if cola == nil {
        return false
    }
    return cola.Set(key, value)
}

func (strg *Storage) Size(table []byte) uint64 {
    var cola = strg.ExistentCOLA(table)
    if cola == nil {
        return 0
    }
    return cola.Size()
}

func (strg *Storage) Keys(table []byte) []string {
    var cola = strg.ExistentCOLA(table)
    if cola == nil {
        return nil
    }
    return cola.Keys()
}

func (strg *Storage) Drop(table []byte) bool {
    var cola = strg.ExistentCOLA(table)
    if cola == nil {
        return true
    }
    cola.Close()
    delete(strg.colas, string(table))
    return os.RemoveAll(DB_DIR + "/" + string(table)) == nil
}

func (strg *Storage) OpenAll() bool {
    fd, err := Open(DB_DIR, O_RDONLY, S_IREAD)
    if err != nil {
        return false
    }
    dir := os.NewFile(uintptr(fd), DB_DIR)
    names, err := dir.Readdirnames(MAX_TABLES)
    if err != nil {
        return false
    }
    for _, name := range names {
        if name != "tag" {
            cola, ok := OpenCOLA(DB_DIR + "/" + name)
            if ok {
                strg.colas[name] = cola
            }
        }
    }
    dir.Close()
    Close(fd)
    return true
}
