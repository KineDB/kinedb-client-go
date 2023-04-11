package types

import (
	"fmt"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

type DoubleType struct {
	FixWidthType
	Value float64
}

func (t *DoubleType) TypeSignature() *TypeSignature {
	return simpleSignature(DOUBLE)
}

func (t *DoubleType) Equals(o Type) bool {
	other, isSameType := o.(*DoubleType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *DoubleType) ToBytes() []byte {
	return DoubleTypeToBytes(t)
}

func (t *DoubleType) GetStandardType() StandardType {
	return DOUBLE
}

func (t *DoubleType) GetFloatValue() float64 {
	return t.Value
}

func (t *DoubleType) CanCompatible(toType StandardType) bool {
	switch toType {
	case DOUBLE:
		return true
	}
	return false
}

func (t *DoubleType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case TINYINT, SMALLINT, INTEGER, BIGINT, VARCHAR, BOOLEAN:
		return true
	}
	return false
}

func (t *DoubleType) CastTo(toType StandardType) Type {
	switch toType {
	case TINYINT:
		return &TinyintType{
			Value: int8(t.Value),
		}
	case SMALLINT:
		return &SmallintType{
			Value: int16(t.Value),
		}
	case INTEGER:
		return &IntegerType{
			Value: int32(t.Value),
		}
	case BIGINT:
		return &BigintType{
			Value: int64(t.Value),
		}
	case DOUBLE:
		return t
	case DECIMAL:
		return CastToDecimal(t)
	case VARCHAR:
		return &VarcharType{
			Value: fmt.Sprintf("%v", t.Value),
		}
	case BOOLEAN:
		var val = false
		if t.Value != 0 {
			val = true
		}
		return &BooleanType{
			Value: val,
		}
	case CHAR:
		return &CharType{
			Value: fmt.Sprintf("%v", t.Value),
		}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
