package types

import (
	"strconv"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

type TinyintType struct {
	FixWidthType
	Value int8
}

func (t *TinyintType) TypeSignature() *TypeSignature {
	return simpleSignature(TINYINT)
}

func (t *TinyintType) Equals(o Type) bool {
	other, isSameType := o.(*TinyintType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *TinyintType) ToBytes() []byte {
	return TinyintTypeToBytes(t)
}

func (t *TinyintType) GetStandardType() StandardType {
	return TINYINT
}

func (t *TinyintType) CanCompatible(toType StandardType) bool {
	switch toType {
	case TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE:
		return true
	}
	return false
}

func (t *TinyintType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case VARCHAR, BOOLEAN:
		return true
	}
	return false
}

func (t *TinyintType) CastTo(toType StandardType) Type {
	switch toType {
	case TINYINT:
		return t
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
		return &DoubleType{
			Value: float64(t.Value),
		}
	case VARCHAR:
		return &VarcharType{
			Value: strconv.FormatInt(int64(t.Value), 10),
		}
	case BOOLEAN:
		var val = false
		if t.Value != 0 {
			val = true
		}
		return &BooleanType{
			Value: val,
		}
	case DECIMAL:
		return CastToDecimal(t)
	case CHAR:
		return &CharType{
			Value: strconv.FormatInt(int64(t.Value), 10),
		}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
