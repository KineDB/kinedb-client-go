package types

import (
	"fmt"
	"strconv"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

type DecimalType struct {
	FixWidthType
	precision        int32
	scale            int32
	negativeSignFlag bool
}

func (t *DecimalType) TypeSignature() *TypeSignature {
	var negativeSign string
	if t.negativeSignFlag {
		negativeSign = "-"
	}
	return &TypeSignature{
		Signature: fmt.Sprintf("%s(%s%d,%d)", DECIMAL, negativeSign, t.precision, t.scale),
		Base:      string(DECIMAL),
	}
}

func (t *DecimalType) Equals(o Type) bool {
	other, isSameType := o.(*DecimalType)
	if !isSameType {
		return false
	}
	return t.precision == other.precision && t.scale == other.scale && t.negativeSignFlag == other.negativeSignFlag
}

func (t *DecimalType) ToBytes() []byte {
	return DecimalTypeToBytes(t)
}

func (t *DecimalType) GetStandardType() StandardType {
	return DECIMAL
}

func (t *DecimalType) CanCompatible(toType StandardType) bool {
	switch toType {
	case DECIMAL:
		return true
	}
	return false
}

func (t *DecimalType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case VARCHAR:
		return true
	}
	return false
}

func (t *DecimalType) CastTo(toType StandardType) Type {
	negativeSign := ""
	if t.negativeSignFlag {
		negativeSign = "-"
	}
	switch toType {
	case DECIMAL:
		return t
	case VARCHAR:
		return &VarcharType{
			Value: fmt.Sprintf("%s%v.%v", negativeSign, t.precision, t.scale),
		}
	case DOUBLE:
		v, convtErr := strconv.ParseFloat(fmt.Sprintf("%s%v.%v", negativeSign, t.precision, t.scale), 64)
		if convtErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &DoubleType{
			Value: v,
		}
	case CHAR:
		return &CharType{
			Value: fmt.Sprintf("%s%v.%v", negativeSign, t.precision, t.scale),
		}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
