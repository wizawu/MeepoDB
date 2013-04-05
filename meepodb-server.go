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
    "net"
    "strconv"
    "strings"
    "syscall"
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
    port, err := strconv.Atoi(flag.Arg(0))
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
    var ok bool = false
    for _, addr := range addrs {
        s := strings.Split(addr.String(), "/")[0] + ":" + flag.Arg(0)
        for _, svr := range meepodb.SERVERS {
            if s == svr {
                ok = true
                println("Hi, MeepoDB on " + svr)
            }
        }
    }
    if !ok {
        println("Your address is not one of the servers.")
        return
    }

    /* Initialize database */
    var dir string = meepodb.DB_DIR
    err = syscall.Chdir(dir)
    /* If database does not exist... */
    if err != nil {
        err = syscall.Mkdir(dir, meepodb.S_IWALL)
        if err != nil {
            panic(err)
        }
    }
    /* If database exists... */

//  meepodb.Reallocate(port)
    meepodb.StartServer(port)
}
