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
    "flag"
    "net"
    "os"
    "sort"
    "./meepodb"
)

var numberOfServers = len(meepodb.SERVERS[:])
var servers = make([](*net.TCPConn), numberOfServers)
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

func get(table, key, value []byte) {
}

func set(table, key, value []byte) {
    var location uint64 = meepodb.HashTableKey(table, key)
    println("Hash location:", location)

}

func drop(table []byte) {
}

func quit() {
}

func main() {
    sort.Sort(sort.StringSlice(meepodb.SERVERS[:]))
    /* Disable replica if the number of servers is less than 3 */
    if meepodb.REPLICA && numberOfServers < 3 {
        meepodb.REPLICA = false
    }
    /* Connect to servers */
    var err error
    for i, _ := range servers {
        servers[i], err = net.Dial("tcp", meepodb.SERVERS[i])
        if err == nil {
            println("Connected to", meepodb.SERVERS[i])
        } else {
            println("Cannot connected to", meepodb.SERVERS[i])
        }
    }
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
                if len(tokens) != 4 {
                    println("*", "GET [TABLE] [KEY] [VALUE]")
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
            case "GET"  : get(tokens[1], tokens[2], tokens[3])
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
