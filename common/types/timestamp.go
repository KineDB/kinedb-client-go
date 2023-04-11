package types

import (
	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

// TIMESTAMP is stored as milliseconds from 1970-01-01T00:00:00 UTC.
type TimestampType struct {
	FixWidthType
	Value int64
}

func (t *TimestampType) TypeSignature() *TypeSignature {
	return simpleSignature(TIMESTAMP)
}

func (t *TimestampType) Equals(o Type) bool {
	other, isSameType := o.(*TimestampType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *TimestampType) ToBytes() []byte {
	return TimestampTypeToBytes(t)
}

func (t *TimestampType) GetLongValue() int64 {
	return t.Value
}

func (t *TimestampType) GetStandardType() StandardType {
	return TIMESTAMP
}

func (t *TimestampType) CanCompatible(toType StandardType) bool {
	switch toType {
	case TIMESTAMP:
		return true
	}
	return false
}

func (t *TimestampType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case DATE:
		return true
	}
	return false
}

func (t *TimestampType) CastTo(toType StandardType) Type {
	switch toType {
	case DATE:
		return &DateType{
			Value: int32(t.Value / (1000 * 24 * 60 * 60)),
		}
	case TIMESTAMP:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
