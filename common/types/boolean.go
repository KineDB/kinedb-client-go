package types

import (
	"github.com/KineDB/kinedb-client-go/common/errors"
)

type BooleanType struct {
	FixWidthType
	Value bool
}

func (t *BooleanType) TypeSignature() *TypeSignature {
	return simpleSignature(BOOLEAN)
}

func (t *BooleanType) GetStandardType() StandardType {
	return BOOLEAN
}

func (t *BooleanType) Equals(o Type) bool {
	other, isSameType := o.(*BooleanType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *BooleanType) ToBytes() []byte {
	return BooleanTypeToBytes(t)
}

func (t *BooleanType) CanCompatible(toType StandardType) bool {
	switch toType {
	case BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE:
		return true
	}
	return false
}

func (t *BooleanType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case VARCHAR:
		return true
	}
	return false
}

func (t *BooleanType) CastTo(toType StandardType) Type {
	switch toType {
	case BOOLEAN:
		return t
	case TINYINT:
		var val int8 = 0
		if t.Value {
			val = 1
		}
		return &TinyintType{
			Value: val,
		}
	case SMALLINT:
		var val int16 = 0
		if t.Value {
			val = 1
		}
		return &SmallintType{
			Value: val,
		}
	case INTEGER:
		var val int32 = 0
		if t.Value {
			val = 1
		}
		return &IntegerType{
			Value: val,
		}
	case BIGINT:
		var val int64 = 0
		if t.Value {
			val = 1
		}
		return &BigintType{
			Value: val,
		}
	case DOUBLE:
		var val float64 = 0.0
		if t.Value {
			val = 1.0
		}
		return &DoubleType{
			Value: val,
		}
	case VARCHAR:
		var val string = "false"
		if t.Value {
			val = "true"
		}
		return &VarcharType{
			Value: val,
		}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
