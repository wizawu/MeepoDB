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
    . "syscall"
)

/* ========================================================================= *
 *                             USER SETTINGS                                 */

var SERVERS = [...]string {
    "192.168.3.139:6631",
}

const DB_DIR string = "/home/wiza/data/meepodb"

var REPLICA bool = false

/* ========================================================================= */

/*
 *  DO NOT MODIFY THE VALUES BELOW IF YOU HAVE NO IDEA WHAT THEY ARE.
 */
const (
    S_IRALL uint32 = S_IRUSR | S_IRGRP | S_IROTH
    S_IWALL uint32 = S_IWUSR | S_IWGRP | S_IWOTH
    S_IXALL uint32 = S_IXUSR | S_IXGRP | S_IXOTH
    S_IRWXA uint32 = S_IRALL | S_IWALL | S_IXALL

    MAX_RECORDS uint64 = 1 << 12
    BLX_BUF_SIZE int64 = int64(1) << 20 * 16

    MAX_CONNS      int = 1000
    MAX_TABLES     int = 10000
    REPLICA_FACTOR int = 3
)
