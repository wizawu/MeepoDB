package main

import (
    "bytes"
    "flag"
    "fmt"
    "strconv"
    "time"
    "./meepodb"
)

func main() {
    flag.Parse()
    if flag.NArg() != 1 {
        println("PLEASE RUN: meepodb-bench [number]")
        return
    }
    ops, err := strconv.Atoi(flag.Arg(0))
    if err != nil {
        println("PLEASE RUN: meepodb-bench [number]")
        return
    }
    var path = ("/tmp/mpdb_" + strconv.Itoa(int(time.Now().Unix())))
    cola, _ := meepodb.NewCOLA(path)
    fmt.Printf("db dir:\t\t%s\n", path)
    v := bytes.Repeat([]byte("JAVAPYTHON"), 10)
    fmt.Println("key size:\t16 bytes")
    fmt.Printf("value size:\t%d bytes\n", len(v))
    beg := time.Now().UnixNano()
    for i := 1000000001; i <= 1000000000 + ops; i++ {
        k := []byte(strconv.Itoa(i) + "Erlang")
        cola.Set(k, v)
    }
    end := time.Now().UnixNano()
    dura := float32(end - beg) / 1000 / 1000 / 1000
    fmt.Printf("time:\t\t%f sec\n", dura)
    fmt.Printf("write:\t\t%.0f ops/sec\n", float32(ops)/dura)

    var count int
    beg = time.Now().UnixNano()
    for i := 1000000001; i <= 1000000000 + ops; i++ {
        k := []byte(strconv.Itoa(i) + "Erlang")
        v := cola.Get(k)
        if len(v) == 100 {
            count++
        }
    }
    end = time.Now().UnixNano()
    cola.Close()
    dura = float32(end - beg) / 1000 / 1000 / 1000
    fmt.Printf("time:\t\t%f sec\n", dura)
    fmt.Printf("read:\t\t%.0f ops/sec\n", float32(ops)/dura)
    fmt.Printf("read count:\t%d\n", count)
}
