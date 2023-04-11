package types

import (
	"fmt"
	"strings"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

type JsonType struct {
	AbstractVariableWidthType
	Value     string
	MaxLength int32
}

func (t *JsonType) TypeSignature() *TypeSignature {
	return &TypeSignature{
		Signature: fmt.Sprintf("%s(%d)", JSON, t.MaxLength),
		Base:      string(VARCHAR),
	}
}

func (t *JsonType) Equals(o Type) bool {
	// only compare value, ignore there
	other, isSameType := o.(*JsonType)
	if !isSameType {
		return false
	}
	return strings.Compare(t.Value, other.Value) == 0
}

func (t *JsonType) ToBytes() []byte {
	return VarcharTypeToBytes(&VarcharType{
		Value:     t.Value,
		MaxLength: t.MaxLength,
	})
}

func (t *JsonType) GetStandardType() StandardType {
	return JSON
}

func (t *JsonType) CanCompatible(toType StandardType) bool {
	switch toType {
	case VARCHAR:
		return true
	}
	return false
}

func (t *JsonType) CanCastTo(toType StandardType) bool {
	if t.CanCompatible(toType) {
		return true
	}
	switch toType {
	case CHAR:
		return true
	}
	return false
}

func (t *JsonType) CastTo(toType StandardType) Type {
	switch toType {
	case VARCHAR:
		return &VarcharType{
			Value:     t.Value,
			MaxLength: t.MaxLength,
		}
	case CHAR:
		return &CharType{
			Value: t.Value,
		}
	case JSON:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
