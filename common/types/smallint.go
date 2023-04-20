package types

import (
	"strconv"

	"github.com/KineDB/kinedb-client-go/common/errors"
)

type SmallintType struct {
	AbstractIntType
	FixWidthType
	Value int16
}

func (t *SmallintType) TypeSignature() *TypeSignature {
	return simpleSignature(SMALLINT)
}

func (t *SmallintType) Equals(o Type) bool {
	other, isSameType := o.(*SmallintType)
	if !isSameType {
		return false
	}
	return other.Value == t.Value
}

func (t *SmallintType) ToBytes() []byte {
	return SmallintTypeToBytes(t)
}

func (t *SmallintType) GetStandardType() StandardType {
	return SMALLINT
}

func (t *SmallintType) GetIntValue() int16 {
	return t.Value
}

func (t *SmallintType) CanCompatible(toType StandardType) bool {
	switch toType {
	case SMALLINT, INTEGER, BIGINT, DOUBLE:
		return true
	}
	return false
}

func (t *SmallintType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case TINYINT, VARCHAR, BOOLEAN:
		return true
	}
	return false
}

func (t *SmallintType) CastTo(toType StandardType) Type {
	switch toType {
	case TINYINT:
		return &TinyintType{
			Value: int8(t.Value),
		}
	case SMALLINT:
		return t
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
