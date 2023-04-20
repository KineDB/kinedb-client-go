package types

import (
	"github.com/KineDB/kinedb-client-go/common/errors"
)

type LongAndAnyState interface {
	AbstractAggregationStateType
}

// row number and double data
// var _ AbstractAggregationStateType = &LongAndDoubleState{}
var _ LongAndAnyState = &LongAndDoubleState{}

type LongAndDoubleState struct {
	Long   int64
	Double float64
}

func (l *LongAndDoubleState) GetLongValue() int64 {
	return l.Long
}

func (l *LongAndDoubleState) GetStateValue() any {
	return l.Double
}

func (l *LongAndDoubleState) GetStateType() StateType {
	return State_Double
}

func (t *LongAndDoubleState) TypeSignature() *TypeSignature {
	return simpleSignature(AGGREGATION_STATE_TYPE)
}

func (t *LongAndDoubleState) Equals(o Type) bool {
	other, isSameType := o.(*LongAndDoubleState)
	if !isSameType {
		return false
	}
	return other.GetLongValue() == t.GetLongValue() && other.GetStateValue() == t.GetStateValue()
}

func (t *LongAndDoubleState) ToBytes() []byte {
	return AggregationStateToBytes(t)
}

func (t *LongAndDoubleState) GetStandardType() StandardType {
	return AGGREGATION_STATE_TYPE
}

func (t *LongAndDoubleState) CanCompatible(toType StandardType) bool {
	return false
}

func (t *LongAndDoubleState) CanCastTo(toType StandardType) bool {
	return false
}

func (t *LongAndDoubleState) CastTo(toType StandardType) Type {
	switch toType {
	case AGGREGATION_STATE_TYPE:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "LongAndDoubleState Cast failed, type incompatible"))
	}
}

// row count and boolean data
// var _ AbstractAggregationStateType = &LongAndBooleanState{}
var _ LongAndAnyState = &LongAndBooleanState{}

type LongAndBooleanState struct {
	Long    int64
	Boolean *bool
}

func (l *LongAndBooleanState) GetLongValue() int64 {
	return l.Long
}

func (l *LongAndBooleanState) GetStateValue() any {
	return l.Boolean
}

func (l *LongAndBooleanState) GetStateType() StateType {
	return State_Boolean
}

func (t *LongAndBooleanState) TypeSignature() *TypeSignature {
	return simpleSignature(AGGREGATION_STATE_TYPE)
}

func (t *LongAndBooleanState) Equals(o Type) bool {
	other, isSameType := o.(*LongAndBooleanState)
	if !isSameType {
		return false
	}
	return other.GetLongValue() == t.GetLongValue() && other.GetStateValue() == t.GetStateValue()
}

func (t *LongAndBooleanState) ToBytes() []byte {
	return AggregationStateToBytes(t)
}

func (t *LongAndBooleanState) GetStandardType() StandardType {
	return AGGREGATION_STATE_TYPE
}

func (t *LongAndBooleanState) CanCompatible(toType StandardType) bool {
	return false
}

func (t *LongAndBooleanState) CanCastTo(toType StandardType) bool {
	return false
}

func (t *LongAndBooleanState) CastTo(toType StandardType) Type {
	switch toType {
	case AGGREGATION_STATE_TYPE:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "LongAndBooleanState Cast failed, type incompatible"))
	}
}

// row count and value data
// var _ AbstractAggregationStateType = &LongAndValueState{}
var _ LongAndAnyState = &LongAndValueState{}

type LongAndValueState struct {
	Long  int64
	Value Type
}

func (l *LongAndValueState) GetLongValue() int64 {
	return l.Long
}

func (l *LongAndValueState) GetStateValue() any {
	return l.Value
}

func (l *LongAndValueState) GetStateType() StateType {
	return State_Value
}

func (t *LongAndValueState) TypeSignature() *TypeSignature {
	return simpleSignature(AGGREGATION_STATE_TYPE)
}

func (t *LongAndValueState) Equals(o Type) bool {
	other, isSameType := o.(*LongAndValueState)
	if !isSameType {
		return false
	}
	return other.GetLongValue() == t.GetLongValue() && other.GetStateValue() == t.GetStateValue()
}

func (t *LongAndValueState) ToBytes() []byte {
	return AggregationStateToBytes(t)
}

func (t *LongAndValueState) GetStandardType() StandardType {
	return AGGREGATION_STATE_TYPE
}

func (t *LongAndValueState) CanCompatible(toType StandardType) bool {
	return false
}

func (t *LongAndValueState) CanCastTo(toType StandardType) bool {
	return false
}

func (t *LongAndValueState) CastTo(toType StandardType) Type {
	switch toType {
	case AGGREGATION_STATE_TYPE:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "LongAndBooleanState Cast failed, type incompatible"))
	}
}

// row count and result array data
// var _ AbstractAggregationStateType = &LongAndArrayState{}
var _ LongAndAnyState = &LongAndArrayState{}

type LongAndArrayState struct {
	Long  int64
	Array StandardValueArray
}

func (l *LongAndArrayState) GetLongValue() int64 {
	return l.Long
}

func (l *LongAndArrayState) GetStateValue() any {
	return l.Array
}

func (l *LongAndArrayState) GetStateType() StateType {
	return State_StandardValueArray
}

func (t *LongAndArrayState) TypeSignature() *TypeSignature {
	return simpleSignature(AGGREGATION_STATE_TYPE)
}

func (t *LongAndArrayState) Equals(o Type) bool {
	other, isSameType := o.(*LongAndValueState)
	if !isSameType {
		return false
	}
	return other.GetLongValue() == t.GetLongValue() && other.GetStateValue() == t.GetStateValue()
}

func (t *LongAndArrayState) ToBytes() []byte {
	return AggregationStateToBytes(t)
}

func (t *LongAndArrayState) GetStandardType() StandardType {
	return AGGREGATION_STATE_TYPE
}

func (t *LongAndArrayState) CanCompatible(toType StandardType) bool {
	return false
}

func (t *LongAndArrayState) CanCastTo(toType StandardType) bool {
	return false
}

func (t *LongAndArrayState) CastTo(toType StandardType) Type {
	switch toType {
	case AGGREGATION_STATE_TYPE:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "LongAndBooleanState Cast failed, type incompatible"))
	}
}

// row count and boolean data
// var _ AbstractAggregationStateType = &LongAndLongState{}
var _ LongAndAnyState = &LongAndLongState{}

type LongAndLongState struct {
	Long int64
	Data *int64
}

func (l *LongAndLongState) GetLongValue() int64 {
	return l.Long
}

func (l *LongAndLongState) GetStateValue() any {
	return l.Data
}

func (l *LongAndLongState) GetStateType() StateType {
	return State_Long
}

func (t *LongAndLongState) TypeSignature() *TypeSignature {
	return simpleSignature(AGGREGATION_STATE_TYPE)
}

func (t *LongAndLongState) Equals(o Type) bool {
	other, isSameType := o.(*LongAndValueState)
	if !isSameType {
		return false
	}
	return other.GetLongValue() == t.GetLongValue() && other.GetStateValue() == t.GetStateValue()
}

func (t *LongAndLongState) ToBytes() []byte {
	return AggregationStateToBytes(t)
}

func (t *LongAndLongState) GetStandardType() StandardType {
	return AGGREGATION_STATE_TYPE
}

func (t *LongAndLongState) CanCompatible(toType StandardType) bool {
	return false
}

func (t *LongAndLongState) CanCastTo(toType StandardType) bool {
	return false
}

func (t *LongAndLongState) CastTo(toType StandardType) Type {
	switch toType {
	case AGGREGATION_STATE_TYPE:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "LongAndBooleanState Cast failed, type incompatible"))
	}
}

// row count and pair array data
// var _ AbstractAggregationStateType = &LongAndPairState{}
var _ LongAndAnyState = &LongAndPairState{}

type LongAndPairState struct {
	Long int64
	Pair *Pair
}

func (l *LongAndPairState) GetLongValue() int64 {
	return l.Long
}

func (l *LongAndPairState) GetStateValue() any {
	return l.Pair
}

func (l *LongAndPairState) GetStateType() StateType {
	return State_Pair
}

func (t *LongAndPairState) TypeSignature() *TypeSignature {
	return simpleSignature(AGGREGATION_STATE_TYPE)
}

func (t *LongAndPairState) Equals(o Type) bool {
	other, isSameType := o.(*LongAndValueState)
	if !isSameType {
		return false
	}
	return other.GetLongValue() == t.GetLongValue() && other.GetStateValue() == t.GetStateValue()
}

func (t *LongAndPairState) ToBytes() []byte {
	return AggregationStateToBytes(t)
}

func (t *LongAndPairState) GetStandardType() StandardType {
	return AGGREGATION_STATE_TYPE
}

func (t *LongAndPairState) CanCompatible(toType StandardType) bool {
	return false
}

func (t *LongAndPairState) CanCastTo(toType StandardType) bool {
	return false
}

func (t *LongAndPairState) CastTo(toType StandardType) Type {
	switch toType {
	case AGGREGATION_STATE_TYPE:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "LongAndBooleanState Cast failed, type incompatible"))
	}
}

// row count and pair array data
// var _ AbstractAggregationStateType = &LongAndPairArrayState{}
var _ LongAndAnyState = &LongAndPairArrayState{}

type LongAndPairArrayState struct {
	Long  int64
	Array PairArray
}

func (l *LongAndPairArrayState) GetLongValue() int64 {
	return l.Long
}

func (l *LongAndPairArrayState) GetStateValue() any {
	return l.Array
}

func (l *LongAndPairArrayState) GetStateType() StateType {
	return State_PairArray
}

func (t *LongAndPairArrayState) TypeSignature() *TypeSignature {
	return simpleSignature(AGGREGATION_STATE_TYPE)
}

func (t *LongAndPairArrayState) Equals(o Type) bool {
	other, isSameType := o.(*LongAndValueState)
	if !isSameType {
		return false
	}
	return other.GetLongValue() == t.GetLongValue() && other.GetStateValue() == t.GetStateValue()
}

func (t *LongAndPairArrayState) ToBytes() []byte {
	return AggregationStateToBytes(t)
}

func (t *LongAndPairArrayState) GetStandardType() StandardType {
	return BIGINT
}

func (t *LongAndPairArrayState) CanCompatible(toType StandardType) bool {
	return false
}

func (t *LongAndPairArrayState) CanCastTo(toType StandardType) bool {
	return false
}

func (t *LongAndPairArrayState) CastTo(toType StandardType) Type {
	switch toType {
	case AGGREGATION_STATE_TYPE:
		return t
	default:
		panic(errors.New(errors.TypeIncompatible, "LongAndBooleanState Cast failed, type incompatible"))
	}
}
