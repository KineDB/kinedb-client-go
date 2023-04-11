package types

import (
	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

// DATETIME is stored as milliseconds from 1970-01-01T00:00:00 UTC.
type DateTimeType struct {
	FixWidthType
	Value int64
}

func (t *DateTimeType) TypeSignature() *TypeSignature {
	return simpleSignature(DATETIME)
}

func (t *DateTimeType) Equals(o Type) bool {
	other, isSameType := o.(*DateTimeType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *DateTimeType) ToBytes() []byte {
	return DateTimeTypeToBytes(t)
}

func (t *DateTimeType) GetLongValue() int64 {
	return t.Value
}

func (t *DateTimeType) GetStandardType() StandardType {
	return DATETIME
}

func (t *DateTimeType) CanCompatible(toType StandardType) bool {
	switch toType {
	case DATETIME:
		return true
	case TIMESTAMP:
		return true
	}
	return false
}

func (t *DateTimeType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case DATETIME:
		return true
	case TIMESTAMP:
		return true
	case DATE:
		return true
	}
	return false
}

func (t *DateTimeType) CastTo(toType StandardType) Type {
	switch toType {
	case DATE:
		return &DateType{
			Value: int32(t.Value / (1000 * 24 * 60 * 60)),
		}
	case DATETIME:
		return t
	case TIMESTAMP:
		return &TimestampType{Value: t.Value}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
