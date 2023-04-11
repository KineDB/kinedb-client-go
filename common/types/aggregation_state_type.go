package types

import "github.com/industry-tenebris/kinedb-goclient/common/errors"

type StateType string

const (
	State_Double             StateType = "DOUBLE"
	State_Boolean            StateType = "BOOLEAN"
	State_Value              StateType = "Value"
	State_StandardValueArray StateType = "StandardValueArray"
	State_Long               StateType = "Long"
	State_Pair               StateType = "Pair"
	State_PairArray          StateType = "PairArray"
)

// aggregation state type
func StateTypeToCode(t StateType) byte {
	switch t {
	case State_Boolean:
		return 0
	case State_Double:
		return 1
	case State_Long:
		return 2
	case State_Pair:
		return 3
	case State_PairArray:
		return 4
	case State_StandardValueArray:
		return 5
	case State_Value:
		return 6
	default:
		panic(errors.Newf(errors.UnknownType, "StateTypeToCode typeStr unKnown %s", string(t)))
	}
}

func StateTypeFromCode(code byte) StateType {
	switch code {
	case 0:
		return State_Boolean
	case 1:
		return State_Double
	case 2:
		return State_Long
	case 3:
		return State_Pair
	case 4:
		return State_PairArray
	case 5:
		return State_StandardValueArray
	case 6:
		return State_Value
	default:
		panic(errors.Newf(errors.UnknownType, "StateTypeFromCode Code  unKnown %d", code))
	}
}
