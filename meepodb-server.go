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

package main


import (
    "flag"
    "fmt"
    "hash/fnv"
    "net"
    "os"
    "sort"
    "strconv"
    "strings"
    . "syscall"
    "./meepodb"
)

func help() {
    println("PLEASE RUN:\tmeepodb-server [port]")
}

func main() {
    /* Get port number from args */
    flag.Parse()
    if flag.NArg() != 1 {
        help()
        return
    }
    _, err := strconv.Atoi(flag.Arg(0))
    if err != nil {
        help()
        return
    }
    /* Check whether the address is defined in config.go */
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        println("Check network interfaces.")
        return
    }
    var self string
    for _, addr := range addrs {
        s := strings.Split(addr.String(), "/")[0] + ":" + flag.Arg(0)
        for _, svr := range meepodb.SERVERS {
            if s == svr {
                self = svr
                println("Hi, MeepoDB on", self)
            }
        }
    }
    if len(self) == 0 {
        println("Your address is not one of the servers.")
        return
    }
    /* Calculate cluster tag */
    sort.Sort(sort.StringSlice(meepodb.SERVERS[:]))
    hash := fnv.New64a()
    for _, s := range meepodb.SERVERS {
        hash.Write([]byte(s + "&"))
    }
    meepodb.CLUSTER_TAG = hash.Sum64()
    println("cluster tag:", meepodb.CLUSTER_TAG)

    var dir string = meepodb.DB_DIR
    println("db dir:", dir)
    err = Chdir(dir)
    /* If database does not exist... */
    var perm = uint32(meepodb.S_IRALL | meepodb.S_IWALL)
    if err != nil {
        err = Mkdir(dir, meepodb.S_IRWXA)
        if err != nil {
            fmt.Println("mkdir:", err)
            return
        }
        /* Create tag */
        fd, err := Open(dir + "/tag", O_RDWR | O_CREAT, perm)
        if err != nil {
            fmt.Println("create tag:", err)
            return
        }
        n, err := Write(fd, meepodb.Uint64ToBytes(meepodb.CLUSTER_TAG))
        if err != nil || n != 8 {
            fmt.Println("write tag:", err)
            return
        }
        Close(fd)
    }
    /* If database exists... */
    fd, err := Open(dir + "/tag", O_RDWR, perm)
    if err != nil {
        fmt.Println("open tag:", err)
        return
    }
    var buffer = make([]byte, 8)
    n, err := Read(fd, buffer)
    if err != nil || n != 8 {
        fmt.Println("read tag:", err)
        Close(fd)
        return
    }
    var oldtag = meepodb.BytesToUint64(buffer)
    /* If SERVERS in config.go changes... */
    if oldtag != meepodb.CLUSTER_TAG {
        println("reallocating...")
        meepodb.Reallocate(self)
        Seek(fd, 0, os.SEEK_SET)
        n, err = Write(fd, meepodb.Uint64ToBytes(meepodb.CLUSTER_TAG))
        if err != nil || n != 8 {
            println("update tag:", err)
            Close(fd)
            return
        }
    }
    Close(fd)
    meepodb.StartServer(self)
}
