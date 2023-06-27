package types

import (
	"fmt"

	"github.com/KineDB/kinedb-client-go/common/errors"
)

type ArrayType struct {
	AbstractVariableWidthType
	Value  []Type
	Length int
	Type   StandardType
}

func (t *ArrayType) TypeSignature() *TypeSignature {
	return &TypeSignature{
		Signature: fmt.Sprintf("%s(%s,%d)", ARRAY, t.Type, t.Length),
		Base:      string(ARRAY),
	}
}

func (t *ArrayType) Equals(o Type) bool {
	// only compare value, ignore there
	other, isSameType := o.(*ArrayType)
	if !isSameType {
		return false
	}
	if len(t.Value) != len(other.Value) {
		return false
	}
	for index, element := range other.Value {
		if element != t.Value[index] {
			return false
		}
	}
	return true
}

func (t *ArrayType) ToBytes() []byte {
	return ArrayToBytes(t)
}

func (t *ArrayType) GetStandardType() StandardType {
	return ARRAY
}

func (t *ArrayType) CanCompatible(toType StandardType) bool {
	switch toType {
	case ARRAY:
		return true
	}
	return false
}

func (t *ArrayType) CanCastTo(toType StandardType) bool {
	return t.CanCompatible(toType)
}

func (t *ArrayType) CastTo(toType StandardType) Type {
	switch toType {
	case ARRAY:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
}
