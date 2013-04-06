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

const (
/*
    | CMD_CODE       : 7  bits               |  ========
    | TABLE_NAME_LEN : 7  bits               |    Head
    | KEY_LEN        : 20 bits               |   64 bits
    | VALUE_LEN      : 30 bits               |  ========
    | TABLE_NAME     : $TABLE_NAME_LEN bytes |
    | KEY            : $KEY_LEN        bytes |
    | VALUE          : $VALUE_LEN      bytes |
*/
    CMD_CODE_BITS   = 7
    TABLE_NAME_BITS = 7
    KEY_BITS        = 20
    VALUE_BITS      = 30

    MAX_TABLE_NAME_LEN = uint64(1) << TABLE_NAME_BITS - 1
    MAX_KEY_LEN        = uint64(1) << KEY_BITS - 1
    MAX_VALUE_LEN      = uint64(1) << VALUE_BITS -1

    GET_CODE  byte = 0x01
    SET_CODE  byte = 0x02
    DEL_CODE  byte = 0x03
    SIZE_CODE byte = 0x0D
    KEYS_CODE byte = 0x0E
    DROP_CODE byte = 0x0F
    MGET_CODE byte = 0x11
    MSET_CODE byte = 0x12
    MDEL_CODE byte = 0x13
    QUIT_CODE byte = 0x21       /* Client quits */
    WORK_CODE byte = 0x22       /* Server ready */
    RALC_CODE byte = 0x23       /* Reallocate   */
    DSTR_CODE byte = 0x24       /* Distribute   */
    EXPL_CODE byte = 0x25       /* Expel        */
    OK_CODE   byte = 0x30
    ERR_CODE  byte = 0x3F
)

/* Check whether a command is 'MXXX'. */
func MoreCmd(code byte) bool {
    return (code & byte(0x10) > 0)
}

func EncodeHead(code byte, tlen, klen, vlen uint64) []byte {
    var x uint64 = uint64(code)
    x = (x << TABLE_NAME_BITS) | tlen
    x = (x << KEY_BITS       ) | klen
    x = (x << VALUE_BITS     ) | vlen
    return Uint64ToBytes(x)
}

func DecodeHead(head []byte) (byte, uint64, uint64, uint64) {
    var x uint64 = 0
    for i := range head {
        x = (x << 8) | uint64(head[i])
    }
    var vlen uint64 = x & MAX_VALUE_LEN
    x >>= VALUE_BITS
    var klen uint64 = x & MAX_KEY_LEN
    x >>= KEY_BITS
    var tlen uint64 = x & MAX_TABLE_NAME_LEN
    x >>= TABLE_NAME_BITS
    return byte(x), tlen, klen, vlen
}

func BytesToUint64(bytes []byte) uint64 {
    var x uint64
    for _, b := range bytes[0 : 8] {
        x = x << 8 + uint64(b)
    }
    return x
}

func Uint64ToBytes(x uint64) []byte {
    var result [8]byte
    for i := range result {
        k := uint64(56 - 8 * i)
        result[i] = byte(x >> k)
    }
    return result[:]
}
