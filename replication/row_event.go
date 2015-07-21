package replication

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	//"github.com/siddontang/go/hack"
	. "github.com/siddontang/go-mysql/mysql"
)

type TableMapEvent struct {
	TableIDSize int

	TableID uint64

	Flags uint16

	Schema []byte
	Table  []byte

	ColumnCount uint64
	ColumnType  []byte
	ColumnMeta  []uint16

	//len = (ColumnCount + 7) / 8
	NullBitmap []byte
}

func (e *TableMapEvent) Decode(data []byte) error {
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.TableIDSize])
	pos += e.TableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	schemaLength := data[pos]
	pos++

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	tableLength := data[pos]
	pos++

	e.Table = data[pos : pos+int(tableLength)]
	pos += int(tableLength)

	//skip 0x00
	pos++

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	e.ColumnType = data[pos : pos+int(e.ColumnCount)]
	pos += int(e.ColumnCount)

	var err error
	var metaData []byte
	if metaData, _, n, err = LengthEnodedString(data[pos:]); err != nil {
		return err
	}

	if err = e.decodeMeta(metaData); err != nil {
		return err
	}

	pos += n

	if len(data[pos:]) != bitmapByteSize(int(e.ColumnCount)) {
		return io.EOF
	}

	e.NullBitmap = data[pos:]

	return nil
}

func bitmapByteSize(columnCount int) int {
	return int(columnCount+7) / 8
}

// see mysql sql/log_event.h
/*
	0 byte
	MYSQL_TYPE_DECIMAL
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR

	1 byte
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_GEOMETRY

	//maybe
	MYSQL_TYPE_TIME2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIMESTAMP2

	2 byte
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_NEWDECIMAL
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING

	This enumeration value is only used internally and cannot exist in a binlog.
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_ENUM
	MYSQL_TYPE_SET
	MYSQL_TYPE_TINY_BLOB
	MYSQL_TYPE_MEDIUM_BLOB
	MYSQL_TYPE_LONG_BLOB
*/
func (e *TableMapEvent) decodeMeta(data []byte) error {
	pos := 0
	e.ColumnMeta = make([]uint16, e.ColumnCount)
	for i, t := range e.ColumnType {
		switch t {
		case MYSQL_TYPE_STRING:
			var x uint16 = uint16(data[pos]) << 8 //real type
			x += uint16(data[pos+1])              //pack or field length
			e.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_NEWDECIMAL:
			var x uint16 = uint16(data[pos]) << 8 //precision
			x += uint16(data[pos+1])              //decimals
			e.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT:
			e.ColumnMeta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case MYSQL_TYPE_BLOB,
			MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_GEOMETRY:
			e.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_TIME2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP2:
			e.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB:
			return fmt.Errorf("unsupport type in binlog %d", t)
		default:
			e.ColumnMeta[i] = 0
		}
	}

	return nil
}

func (e *TableMapEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Schema: %s\n", e.Schema)
	fmt.Fprintf(w, "Table: %s\n", e.Table)
	fmt.Fprintf(w, "Column count: %d\n", e.ColumnCount)
	fmt.Fprintf(w, "Column type: \n%s", hex.Dump(e.ColumnType))
	fmt.Fprintf(w, "NULL bitmap: \n%s", hex.Dump(e.NullBitmap))
	fmt.Fprintln(w)
}

type RowsEvent struct {
	//0, 1, 2
	Version int

	TableIDSize int
	Tables      map[uint64]*TableMapEvent
	NeedBitmap2 bool

	Table *TableMapEvent

	TableID uint64

	Flags uint16

	//if version == 2
	ExtraData []byte

	//lenenc_int
	ColumnCount uint64
	//len = (ColumnCount + 7) / 8
	ColumnBitmap1 []byte

	//if UPDATE_ROWS_EVENTv1 or v2
	//len = (ColumnCount + 7) / 8
	ColumnBitmap2 []byte

	//rows: invalid: int64, float64, bool, []byte, string
	Rows [][][]byte
}

func (e *RowsEvent) Decode(data []byte) error {
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.TableIDSize])
	pos += e.TableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if e.Version == 2 {
		dataLen := binary.LittleEndian.Uint16(data[pos:])
		pos += 2

		e.ExtraData = data[pos : pos+int(dataLen-2)]
		pos += int(dataLen - 2)
	}

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	bitCount := bitmapByteSize(int(e.ColumnCount))
	e.ColumnBitmap1 = data[pos : pos+bitCount]
	pos += bitCount

	if e.NeedBitmap2 {
		e.ColumnBitmap2 = data[pos : pos+bitCount]
		pos += bitCount
	}

	var ok bool
	e.Table, ok = e.Tables[e.TableID]
	if !ok {
		return fmt.Errorf("invalid table id %d, no correspond table map event", e.TableID)
	}

	var err error

	// ... repeat rows until event-end
	for pos < len(data) {
		if n, err = e.decodeRows(data[pos:], e.Table, e.ColumnBitmap1); err != nil {
			return err
		}
		pos += n

		if e.NeedBitmap2 {
			if n, err = e.decodeRows(data[pos:], e.Table, e.ColumnBitmap2); err != nil {
				return err
			}
			pos += n
		}
	}

	return nil
}

func (e *RowsEvent) decodeRows(data []byte, table *TableMapEvent, bitmap []byte) (int, error) {
	rows := make([][]byte, e.ColumnCount)

	pos := 0

	count := (bitCount(bitmap) + 7) / 8

	nullBitmap := data[pos : pos+count]
	pos += count

	nullbitIndex := 0

	var n int
	var err error
	for i := 0; i < int(e.ColumnCount); i++ {
		isNull := (uint32(nullBitmap[nullbitIndex/8]) >> uint32(nullbitIndex%8)) & 0x01

		if bitGet(bitmap, i) == 0 {
			continue
		}

		if isNull > 0 {
			rows[i] = nil
			nullbitIndex++
			continue
		}

		rows[i], n, err = e.decodeValue(data[pos:], table.ColumnType[i], table.ColumnMeta[i])

		if err != nil {
			return 0, nil
		}
		pos += n

		nullbitIndex++
	}

	e.Rows = append(e.Rows, rows)
	return pos, nil
}

// see mysql sql/log_event.cc log_event_print_value
func (e *RowsEvent) decodeValue(data []byte, tp byte, meta uint16) (v []byte, n int, err error) {
	var length int = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = byte(b0 | 0x30)
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_LONG:
		n = 4
		v = data[0:4]
	case MYSQL_TYPE_TINY:
		n = 1
		v = data[0:1]
	case MYSQL_TYPE_SHORT:
		n = 2
		v = data[0:2]
	case MYSQL_TYPE_INT24:
		n = 3
		v = data[0:3]
	case MYSQL_TYPE_LONGLONG:
		//em, maybe overflow for int64......
		n = 8
		v = data[0:8]
	case MYSQL_TYPE_NEWDECIMAL:
		prec := uint8(meta >> 8)
		scale := uint8(meta & 0xFF)
		v, n, err = decodeDecimal(data, int(prec), int(scale))
	case MYSQL_TYPE_FLOAT:
		n = 4
		v = data[0:4]
	case MYSQL_TYPE_DOUBLE:
		n = 8
		v = data[0:8]
	case MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		n = int(nbits+7) / 8
		v = data[0:n]
	case MYSQL_TYPE_TIMESTAMP:
		n = 4
		v = data[0:4]
	case MYSQL_TYPE_TIMESTAMP2:
		v, n, err = decodeTimestamp2(data, meta)
	case MYSQL_TYPE_DATETIME:
		n = 8
		v = data[0:8]
	case MYSQL_TYPE_DATETIME2:
		v, n, err = decodeDatetime2(data, meta)
	case MYSQL_TYPE_TIME:
		n = 3
		v = data[0:3]
	case MYSQL_TYPE_TIME2:
		v, n, err = decodeTime2(data, meta)
	case MYSQL_TYPE_DATE:
		n = 3
		v = data[0:3]

	case MYSQL_TYPE_YEAR:
		n = 1
		v = data[0:1]
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			v = data[0:1]
			n = 1
		case 2:
			v = data[0:2]
			n = 2
		default:
			err = fmt.Errorf("Unknown ENUM packlen=%d", l)
		}
	case MYSQL_TYPE_SET:
		nbits := meta & 0xFF
		n = int(nbits+7) / 8
		v = data[0:n]
	case MYSQL_TYPE_BLOB:
		switch meta {
		case 1:
			length = int(data[0])
			v = data[0 : 1+length]
			n = length + 1
		case 2:
			length = int(binary.LittleEndian.Uint16(data))
			v = data[0 : 2+length]
			n = length + 2
		case 3:
			length = int(FixedLengthInt(data[0:3]))
			v = data[0 : 3+length]
			n = length + 3
		case 4:
			length = int(binary.LittleEndian.Uint32(data))
			v = data[0 : 4+length]
			n = length + 4
		default:
			err = fmt.Errorf("invalid blob packlen = %d", meta)
		}
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING:
		length = int(meta)
		v, n = decodeString(data, length)
	case MYSQL_TYPE_STRING:
		v, n = decodeString(data, length)
	default:
		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
	}
	return
}

func decodeString(data []byte, length int) (v []byte, n int) {
	if length < 256 {
		length = int(data[0])

		n = int(length) + 1
		v = data[0:n]
	} else {
		length = int(binary.LittleEndian.Uint16(data[0:]))
		n = length + 2
		v = data[0:n]
	}

	return
}

const digitsPerInteger int = 9

var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func decodeDecimal(data []byte, precision int, decimals int) ([]byte, int, error) {
	// fwh mod: let's concerning on the data flow of pos
	
	//see python mysql replication and https://github.com/jeremycole/mysql_binlog
	pos := 0

	integral := (precision - decimals)
	uncompIntegral := int(integral / digitsPerInteger)
	uncompFractional := int(decimals / digitsPerInteger)
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	//binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
	//	uncompFractional*4 + compressedBytes[compFractional]

	//buf := make([]byte, binSize)
	//copy(buf, data[:binSize])

	//must copy the data for later change
	//data = buf

	// Support negative
	// The sign is encoded in the high bit of the the byte
	// But this bit can also be used in the value
	//value := uint32(data[pos])
	//var res bytes.Buffer
	//var mask uint32 = 0
	//if value&0x80 != 0 {
	//	mask = 0
	//} else {
	//	mask = uint32((1 << 32) - 1)
	//	res.WriteString("-")
	//}

	//clear sign
	//data[0] ^= 0x80

	size := compressedBytes[compIntegral]
	if size > 0 {
		//value = uint32(BFixedLengthInt(data[pos:pos+size])) ^ mask
		//res.WriteString(fmt.Sprintf("%d", value))
		pos += size
	}

	/*for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}*/
	pos += uncompIntegral * 4

	//res.WriteString(".")


	/*for i := 0; i < uncompFractional; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}*/
	pos += 4 * uncompFractional

	size = compressedBytes[compFractional]
	if size > 0 {
		//value = uint32(BFixedLengthInt(data[pos:pos+size])) ^ mask
		pos += size

		// we could not use %0*d directly, value is uint32, if compFractional is 2, size is 1, we should only print
		// uint8(value) with width compFractional, not uint32(value), otherwise, the output would be incorrect.
		// res.WriteString(fmt.Sprintf("%0*d", compFractional, value))
		//res.WriteString(fmt.Sprintf("%0*d", compFractional, value%(uint32(size)<<8)))
	}

	//return hack.String(res.Bytes()), pos, nil
	//f, err := strconv.ParseFloat(hack.String(res.Bytes()), 64)
	return data[0:pos], pos, nil
}

func decodeBit(data []byte, nbits int, length int) (value int64, err error) {
	if nbits > 1 {
		switch length {
		case 1:
			value = int64(data[0])
		case 2:
			value = int64(binary.BigEndian.Uint16(data))
		case 3:
			value = int64(BFixedLengthInt(data[0:3]))
		case 4:
			value = int64(binary.BigEndian.Uint32(data))
		case 5:
			value = int64(BFixedLengthInt(data[0:5]))
		case 6:
			value = int64(BFixedLengthInt(data[0:6]))
		case 7:
			value = int64(BFixedLengthInt(data[0:7]))
		case 8:
			value = int64(binary.BigEndian.Uint64(data))
		default:
			err = fmt.Errorf("invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = fmt.Errorf("invalid bit length %d", length)
		} else {
			value = int64(data[0])
		}
	}
	return
}

func decodeTimestamp2(data []byte, dec uint16) ([]byte, int, error) {
	//get timestamp binary length
	n := int(4 + (dec+1)/2)
	return data[0:n], n, nil
}

const DATETIMEF_INT_OFS int64 = 0x8000000000

func decodeDatetime2(data []byte, dec uint16) ([]byte, int, error) {
	//get datetime binary length
	n := int(5 + (dec+1)/2)
	return data[0:n], n, nil
}

const TIMEF_OFS int64 = 0x800000000000
const TIMEF_INT_OFS int64 = 0x800000

func decodeTime2(data []byte, dec uint16) ([]byte, int, error) {
	//time  binary length
	n := int(3 + (dec+1)/2)
	return data[0:n], n, nil
}

func (e *RowsEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Column count: %d\n", e.ColumnCount)

	fmt.Fprintf(w, "Values:\n")
	for _, rows := range e.Rows {
		fmt.Fprintf(w, "--\n")
		for j, d := range rows {
			fmt.Fprintf(w, "%d:%q\n", j, d)
		}
	}
	fmt.Fprintln(w)
}

type RowsQueryEvent struct {
	Query []byte
}

func (e *RowsQueryEvent) Decode(data []byte) error {
	//ignore length byte 1
	e.Query = data[1:]
	return nil
}

func (e *RowsQueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}
