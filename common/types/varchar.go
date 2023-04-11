package types

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
	"github.com/industry-tenebris/kinedb-goclient/common/utils"
)

type VarcharType struct {
	AbstractVariableWidthType
	Value     string
	MaxLength int32
}

func (t *VarcharType) TypeSignature() *TypeSignature {
	return &TypeSignature{
		Signature: fmt.Sprintf("%s(%d)", VARCHAR, t.MaxLength),
		Base:      string(VARCHAR),
	}
}

func (t *VarcharType) Equals(o Type) bool {
	// only compare value, ignore there
	other, isSameType := o.(*VarcharType)
	if !isSameType {
		return false
	}
	return strings.Compare(t.Value, other.Value) == 0
}

func (t *VarcharType) ToBytes() []byte {
	return VarcharTypeToBytes(t)
}

func (t *VarcharType) GetStandardType() StandardType {
	return VARCHAR
}

func (t *VarcharType) CanCompatible(toType StandardType) bool {
	switch toType {
	case VARCHAR:
		return true
	}
	return false
}

func (t *VarcharType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, BOOLEAN, CHAR:
		return true
	}
	return false
}

func (t *VarcharType) CastTo(toType StandardType) Type {
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
		return t
	case CHAR:
		return &CharType{
			Value: t.Value,
		}
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
