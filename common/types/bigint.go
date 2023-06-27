package types

import (
	"strconv"

	"github.com/KineDB/kinedb-client-go/common/errors"
)

type BigintType struct {
	AbstractLongType
	Value int64
}

func (t *BigintType) TypeSignature() *TypeSignature {
	return simpleSignature(BIGINT)
}

func (t *BigintType) Equals(o Type) bool {
	other, isSameType := o.(*BigintType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *BigintType) ToBytes() []byte {
	return BigintTypeToBytes(t)
}

func (t *BigintType) GetLongValue() int64 {
	return t.Value
}

func (t *BigintType) GetStandardType() StandardType {
	return BIGINT
}

func (t *BigintType) CanCompatible(toType StandardType) bool {
	switch toType {
	case BIGINT, DOUBLE:
		return true
	}
	return false
}

func (t *BigintType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case INTEGER, SMALLINT, TINYINT, VARCHAR, BOOLEAN:
		return true
	}
	return false
}

func (t *BigintType) CastTo(toType StandardType) Type {
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
		return t
	case DOUBLE:
		return &DoubleType{
			Value: float64(t.Value),
		}
	case VARCHAR:
		return &VarcharType{
			Value: strconv.FormatInt(t.Value, 10),
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
			Value: strconv.FormatInt(t.Value, 10),
		}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
