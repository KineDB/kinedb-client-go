package types

/*
	SynapseDB Build-in unified Types for SQL
*/

type StandardType string

func (t StandardType) ToString() string {
	return string(t)
}

type Type interface {
	Equals(Type) bool
	TypeSignature() *TypeSignature
	ToBytes() []byte
	GetStandardType() StandardType
	CanCompatible(StandardType) bool
	CanCastTo(StandardType) bool
	CastTo(StandardType) Type
}

type TypeSignature struct {
	Signature string
	Base      string
}

func simpleSignature(s StandardType) *TypeSignature {
	return &TypeSignature{
		Signature: string(s),
		Base:      string(s),
	}
}

/*
	Variable width types
*/

type VariableWidthType interface {
	Type
}

type AbstractVariableWidthType interface {
	VariableWidthType
}

/*
Fix width types
*/
type FixWidthType interface {
	Type
}

type AbstractIntType interface {
	GetIntValue() int32
}

type AbstractLongType interface {
	FixWidthType
	GetLongValue() int64
}

type AbstractFloat interface {
	GetFloatValue() float64
}

type AbstractAggregationStateType interface {
	Type
	GetLongValue() int64
	GetStateValue() any
	GetStateType() StateType
}
