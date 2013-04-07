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
    "bufio"
    "bytes"
    "fmt"
    "net"
    "os"
    "sort"
    . "syscall"
    "./meepodb"
)

var numberOfServers = uint64(len(meepodb.SERVERS[:]))
var servers = make([]int, numberOfServers)
var stdin = bufio.NewReader(os.Stdin)
var lineNumber int = 1

func ReadTokensInLine() []([]byte) {
    line, err := stdin.ReadBytes('\n')
    if err != nil {
        return nil
    }
    /* Strip LF character */
    line = line[: len(line) - 1]
    tokenizer := bufio.NewReader(bytes.NewBuffer(line))
    /* Redundant spaces should be less than 256 */
    result := make([]([]byte), 256)
    var count int = 0
    for {
        token, err := tokenizer.ReadBytes(' ')
        /* Strip space character */
        if err == nil {
            token = token[: len(token) - 1]
        }
        if len(token) > 0 {
            result[count] = token
            count++
        }
        /* Encounter end-of-line */
        if err != nil {
            break
        }
    }
    return result[:count]
}

func sendRequest(i uint64, request []byte) bool {
    if servers[i] == -1 {
        /* Try dailing again */
        addr, err := net.ResolveTCPAddr("tcp", meepodb.SERVERS[i])
        conn, err := net.DialTCP("tcp", nil, addr)
        if err != nil {
            println("* Failed on", meepodb.SERVERS[i])
            return false
        }
        conn.SetKeepAlive(true)
        conn.SetNoDelay(true)
        file, _ := conn.File()
        servers[i] = int(file.Fd())
    }
    n, err := Write(servers[i], request)
    if err != nil || n != len(request) {
        println("* Failed on", meepodb.SERVERS[i])
        return false
    }
    return true
}

func nonvoidPrint(str []byte) {
    if len(str) == 0 {
        fmt.Println("<nil>")
        return
    }
    fmt.Printf("%s\n", str)
}

func getFrom(i uint64, request []byte) ([]byte, bool) {
    var ok bool = sendRequest(i, request)
    if !ok {
        return nil, false
    }
    var head = make([]byte, 8)
    n, err := Read(servers[i], head)
    if err != nil || n != 8 {
        return nil, false
    }
    code, _, _, vlen := meepodb.DecodeHead(head)
    if code == meepodb.ERR_CODE {
        return nil, false
    }
    if vlen == 0 {
        return []byte(""), true
    }
    var value = make([]byte, vlen)
    n, err = Read(servers[i], value)
    if err != nil || n != int(vlen) {
        return nil, false
    }
    return value, true
}

func get(table, key []byte) {
    var ok bool
    var v1, v2, v3 []byte
    var request []byte = meepodb.EncodeGet(table, key)
    var i uint64 = meepodb.HashTableKey(table, key)
    println("Hash location:", i)
    i = i % numberOfServers
    var i2 = (i + 1) % numberOfServers
    var i3 = (i + 2) % numberOfServers
    v1, ok = getFrom(i, request)
    if ok {
        /* If v1 ok... */
        if meepodb.REPLICA == false {
            nonvoidPrint(v1)
        } else {
            v2, ok = getFrom(i2, request)
            if ok {
                /* If v1 == v2... */
                if bytes.Compare(v1, v2) == 0 {
                    nonvoidPrint(v1)
                } else {
                    v3, ok = getFrom(i3, request)
                    if ok {
                        if bytes.Compare(v1, v3) == 0 || bytes.Compare(v2, v3) == 0 {
                            nonvoidPrint(v3)
                        } else {
                            nonvoidPrint(v1)
                        }
                    } else {
                        /* If v3 not ok... */
                        println("* Failed on", meepodb.SERVERS[i3])
                        nonvoidPrint(v1)
                    }
                }
            } else {
                /* If v2 not ok... */
                println("* Failed on", meepodb.SERVERS[i2])
                nonvoidPrint(v1)
            }
        }
    } else {
        /* If v1 not ok... */
        println("* Failed on", meepodb.SERVERS[i])
        if meepodb.REPLICA {
            v2, ok = getFrom(i2, request)
            if ok {
                /* If v2 ok... */
                nonvoidPrint(v2)
            } else {
                println("* Failed on", meepodb.SERVERS[i2])
                v3, ok = getFrom(i3, request)
                if ok {
                    /* If v3 ok... */
                    nonvoidPrint(v3)
                } else {
                    println("* Failed on", meepodb.SERVERS[i3])
                }
            }
        }
    }
}

func set(table, key, value []byte) {
    var request []byte = meepodb.EncodeSet(table, key, value)
    var i uint64 = meepodb.HashTableKey(table, key)
    println("Hash location:", i)
    i = i % numberOfServers
    sendRequest(i, request)
    if meepodb.REPLICA == false {
        return
    }
    /* Replica 1 */
    i = (i + 1) % numberOfServers
    sendRequest(i, request)
    /* Replica 2 */
    i = (i + 1) % numberOfServers
    sendRequest(i, request)
}

func drop(table []byte) {
    var request []byte = meepodb.EncodeDrop(table)
    for i := range servers {
        sendRequest(uint64(i), request)
    }
}

func quit() {
    var request []byte = meepodb.EncodeSym(meepodb.QUIT_CODE)
    for _, fd := range servers {
        if fd != - 1{
            Write(fd, request)
            Close(fd)
        }
    }
}

func main() {
    sort.Sort(sort.StringSlice(meepodb.SERVERS[:]))
    /* Disable replica if the number of servers is less than 3 */
    if meepodb.REPLICA && numberOfServers < 3 {
        meepodb.REPLICA = false
    }
    /* Connect to servers */
    for i, _ := range servers {
        addr, err := net.ResolveTCPAddr("tcp", meepodb.SERVERS[i])
        conn, err := net.DialTCP("tcp", nil, addr)
        if err == nil {
            println("Connected to", meepodb.SERVERS[i])
            conn.SetKeepAlive(true)
            conn.SetNoDelay(true)
            defer conn.Close()
            file, _ := conn.File()
            servers[i] = int(file.Fd())
        } else {
            println("Cannot connected to", meepodb.SERVERS[i])
            /* Set server socket to -1 if connection fails */
            servers[i] = -1
        }
    }
    /* Start shell */
    println("\nMeepoDB Shell")
    for {
        print(lineNumber, "> ")
        tokens := ReadTokensInLine()
        if len(tokens) == 0 {
            continue
        }
        /* Test tokenizer
        for _, t := range tokens {
            print(string(t), "$")
        }
        println() */

        /* Check command format */
        switch string(tokens[0]) {
            case "GET":
                if len(tokens) != 3 {
                    println("*", "GET [TABLE] [KEY]")
                    continue
                }
            case "SET":
                if len(tokens) != 4 {
                    println("*", "SET [TABLE] [KEY] [VALUE]")
                    continue
                }
            case "DEL":
                if len(tokens) != 3 {
                    println("*", "DEL [TABLE] [KEY]")
                    continue
                }
            case "DROP":
                if len(tokens) != 2 {
                    println("*", "DROP [TABLE]")
                    continue
                }
            case "QUIT":
                if len(tokens) != 1 {
                    println("*", "QUIT")
                    continue
                }
            default:
                println("*", "Unknown command")
                continue
        }
        /* Send command */
        switch string(tokens[0]) {
            case "GET"  : get(tokens[1], tokens[2])
            case "SET"  : set(tokens[1], tokens[2], tokens[3])
            case "DEL"  : set(tokens[1], tokens[2], []byte(""))
            case "DROP" : drop(tokens[1])
            case "QUIT" : quit()
        }
        /* Exit the client */
        if string(tokens[0]) == "QUIT" {
            break
        }
        lineNumber++
    }
}
