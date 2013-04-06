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
    "os"
    "./meepodb"
)

var stdin = bufio.NewReader(os.Stdin)
var lineNumber int = 1

func ReadTokensInLine() []([]byte) {
    line, err := stdin.ReadBytes('\n')
    if err != nil {
        return nil
    }
    line = line[: len(line) - 1]
    tokenizer := bufio.NewReader(bytes.NewBuffer(line))
    result := make([]([]byte), meepodb.MAX_MGET_KEYS + 2)
    var count int = 0
    for {
        token, err := tokenizer.ReadBytes(' ')
        if err == nil {
            token = token[: len(token) - 1]
        }
        if len(token) > 1 {
            result[count] = token
            count++
        }
        if err != nil {
            break
        }
    }
    return result[:count]
}

func help() {
    println("PLEASE RUN:\tmeepodb-cli [IP:port]")
}

func main() {
    flag.Parse()
    if flag.NArg() != 1 {
        help()
        return
    }
    println("Connect to MeepoDB on", flag.Arg(0))
    println("\nMeepoDB Shell")
    for {
        print(lineNumber, "> ")
        tokens := ReadTokensInLine()
        for _, t := range tokens {
            print(string(t), "$")
        }
        println()
        lineNumber++
    }
}
