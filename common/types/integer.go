package types

import (
	"strconv"

	"github.com/KineDB/kinedb-client-go/common/errors"
)

type IntegerType struct {
	AbstractIntType
	Value int32
}

func (t *IntegerType) TypeSignature() *TypeSignature {
	return simpleSignature(INTEGER)
}

func (t *IntegerType) Equals(o Type) bool {
	other, isSameType := o.(*IntegerType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *IntegerType) ToBytes() []byte {
	return IntegerTypeToBytes(t)
}

func (t *IntegerType) GetIntValue() int32 {
	return t.Value
}

func (t *IntegerType) GetStandardType() StandardType {
	return INTEGER
}

func (t *IntegerType) CanCompatible(toType StandardType) bool {
	switch toType {
	case INTEGER, BIGINT, DOUBLE:
		return true
	}
	return false
}

func (t *IntegerType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case SMALLINT, TINYINT, VARCHAR, BOOLEAN:
		return true
	}
	return false
}

func (t *IntegerType) CastTo(toType StandardType) Type {
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
		return t
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
