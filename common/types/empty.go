package types

import (
	"github.com/KineDB/kinedb-client-go/common/errors"
)

// EmptyType it is a special Type, represents no data/Null
type EmptyType struct {
	AbstractIntType
}

func (t *EmptyType) TypeSignature() *TypeSignature {
	return simpleSignature(EMPTY)
}

func (t *EmptyType) Equals(o Type) bool {
	_, isSameType := o.(*EmptyType)
	if !isSameType {
		return false
	}
	return true
}

func (t *EmptyType) ToBytes() []byte {
	return []byte{}
}

func (t *EmptyType) GetStandardType() StandardType {
	return EMPTY
}

func (t *EmptyType) CanCompatible(toType StandardType) bool {
	if toType != UNKNOWN {
		return true
	}
	return false
}

func (t *EmptyType) CanCastTo(toType StandardType) bool {
	return t.CanCompatible(toType)
}

func (t *EmptyType) CastTo(toType StandardType) Type {
	if toType == UNKNOWN {
		panic(errors.New(errors.TypeIncompatible, "Cast failed, type incompatible"))
	}
	return t
}
