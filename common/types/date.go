package types

import (
	"time"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

// A time is stored as milliseconds from midnight on 1970-01-01T00:00:00 .
type DateType struct {
	AbstractIntType
	Value int32
}

func (t *DateType) TypeSignature() *TypeSignature {
	return simpleSignature(DATE)
}

func (t *DateType) Equals(o Type) bool {
	other, isSameType := o.(*DateType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *DateType) ToBytes() []byte {
	return DateTypeToBytes(t)
}

func (t *DateType) GetIntValue() int32 {
	return t.Value
}

func (t *DateType) GetStandardType() StandardType {
	return DATE
}

func (t *DateType) CanCompatible(toType StandardType) bool {
	switch toType {
	case DATE, TIMESTAMP:
		return true
	}
	return false
}

func (t *DateType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	return false
}

func (t *DateType) CastTo(toType StandardType) Type {
	switch toType {
	case DATE:
		return t
	case TIMESTAMP:
		return &TimestampType{
			Value: time.Unix(int64(t.Value*24*60*60), 0).UnixMilli(),
		}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
	panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
}
