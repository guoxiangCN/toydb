package toydb

import "encoding/binary"

const (
	PUT uint32 = 0
	DEL uint32 = 1
)

type DbItem struct {
	// hdr
	KeySize uint64
	ValSize uint64
	Flag    uint32

	// data
	Key []byte
	Val []byte
}

const DbItemHdrSize = 8 + 8 + 4

func newDBItem(key, value []byte, flag uint32) *DbItem {
	return &DbItem{
		Key:     key,
		Val:     value,
		KeySize: uint64(uint32(len(key))),
		ValSize: uint64(uint32(len(value))),
		Flag:    flag,
	}
}

func (it *DbItem) Size() uint64 {
	return DbItemHdrSize + it.KeySize + it.ValSize
}

func (it *DbItem) Encode() ([]byte, error) {
	buf := make([]byte, it.Size())
	binary.BigEndian.PutUint64(buf[0:8], it.KeySize)
	binary.BigEndian.PutUint64(buf[8:16], it.ValSize)
	binary.BigEndian.PutUint32(buf[16:20], it.Flag)
	copy(buf[DbItemHdrSize:DbItemHdrSize+it.KeySize], it.Key)
	copy(buf[DbItemHdrSize+it.KeySize:], it.Val)
	return buf, nil
}

func DecodeDbItemHdr(buf []byte) (*DbItem, error) {
	ks := binary.BigEndian.Uint64(buf[0:8])
	vs := binary.BigEndian.Uint64(buf[8:16])
	flag := binary.BigEndian.Uint32(buf[16:20])
	return &DbItem{KeySize: ks, ValSize: vs, Flag: flag, Key: nil, Val: nil}, nil
}
