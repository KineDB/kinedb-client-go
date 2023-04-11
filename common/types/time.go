package types

import (
	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

// A time is stored as milliseconds from midnight on 1970-01-01T00:00:00
type TimeType struct {
	AbstractLongType
	Value int64
}

func (t *TimeType) TypeSignature() *TypeSignature {
	return simpleSignature(TIME)
}

func (t *TimeType) Equals(o Type) bool {
	other, isSameType := o.(*TimeType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *TimeType) ToBytes() []byte {
	return TimeTypeToBytes(t)
}

func (t *TimeType) GetLongValue() int64 {
	return t.Value
}

func (t *TimeType) GetStandardType() StandardType {
	return TIME
}

func (t *TimeType) CanCompatible(toType StandardType) bool {
	switch toType {
	case TIME:
		return true
	}
	return false
}

func (t *TimeType) CanCastTo(toType StandardType) bool {
	return t.CanCompatible(toType)
}

func (t *TimeType) CastTo(toType StandardType) Type {
	switch toType {
	case TIME:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
	panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
}
