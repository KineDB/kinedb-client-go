package types

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/KineDB/kinedb-client-go/common/errors"
	"github.com/KineDB/kinedb-client-go/common/utils"
)

type CharType struct {
	AbstractVariableWidthType
	Value     string
	MaxLength uint16
}

func (t *CharType) TypeSignature() *TypeSignature {
	return &TypeSignature{
		Signature: fmt.Sprintf("%s(%d)", CHAR, t.MaxLength),
		Base:      string(CHAR),
	}
}

func (t *CharType) Equals(o Type) bool {
	other, isSameType := o.(*CharType)
	if !isSameType {
		return false
	}
	return strings.Compare(other.Value, t.Value) == 0
}

func (t *CharType) ToBytes() []byte {
	return CharTypeToBytes(t)
}

func (t *CharType) GetStandardType() StandardType {
	return CHAR
}

func (t *CharType) CanCompatible(toType StandardType) bool {
	switch toType {
	case CHAR, VARCHAR:
		return true
	}
	return false
}

func (t *CharType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, BOOLEAN:
		return true
	}
	return false
}

func (t *CharType) CastTo(toType StandardType) Type {
	switch toType {
	case TINYINT:
		val, convErr := strconv.Atoi(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &TinyintType{
			Value: int8(val),
		}
	case SMALLINT:
		val, convErr := strconv.Atoi(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &SmallintType{
			Value: int16(val),
		}
	case INTEGER:
		val, convErr := strconv.Atoi(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &IntegerType{
			Value: int32(val),
		}
	case BIGINT:
		val, convErr := strconv.Atoi(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &BigintType{
			Value: int64(val),
		}
	case DOUBLE:
		val, convErr := strconv.ParseFloat(t.Value, 64)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &DoubleType{
			Value: val,
		}
	case VARCHAR:
		return &VarcharType{
			Value: t.Value,
		}
	case CHAR:
		return t
	case BOOLEAN:
		val, convErr := strconv.ParseBool(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &BooleanType{
			Value: val,
		}
	case TIMESTAMP:
		val, convErr := utils.ParseTimeStrToMilliSeconds(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &TimestampType{
			Value: val,
		}
	case TIME:
		val, convErr := utils.ParseTimeStrToMilliSeconds(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &TimeType{
			Value: val,
		}
	case DATE:
		val, convErr := utils.ParseDateStrToDays(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &DateType{
			Value: val,
		}
	case DECIMAL:
		return StringToDecimal(t.Value)
	case DATETIME:
		val, convErr := utils.ParseTimeStrToMilliSeconds(t.Value)
		if convErr != nil {
			panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
		}
		return &DateTimeType{
			Value: val,
		}
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
