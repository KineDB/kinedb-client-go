package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
	"github.com/industry-tenebris/kinedb-goclient/common/utils"
)

/*
Conversion of SynapseDB Unified Types to Bytes
*/
func BytesToBigintType(data []byte) *BigintType {
	return &BigintType{
		Value: BytesToInt64(data),
	}
}

func BigintTypeToBytes(v *BigintType) []byte {
	return Int64ToBytes(v.Value)
}

func BytesToDateType(data []byte) *DateType {
	return &DateType{
		Value: BytesToInt32(data),
	}
}

func DateTypeToBytes(v *DateType) []byte {
	return Int32ToBytes(v.Value)
}

func BytesToIntegerType(data []byte) *IntegerType {
	return &IntegerType{
		Value: BytesToInt32(data),
	}
}

func IntegerTypeToBytes(v *IntegerType) []byte {
	return Int32ToBytes(v.Value)
}

func BytesToTimeType(data []byte) *TimeType {
	return &TimeType{
		Value: BytesToInt64(data),
	}
}

func TimeTypeToBytes(v *TimeType) []byte {
	return Int64ToBytes(v.Value)
}

func BytesToBooleanType(data []byte) *BooleanType {
	return &BooleanType{
		Value: BytesToBool(data),
	}
}

func BooleanTypeToBytes(v *BooleanType) []byte {
	return boolToBytes(v.Value)
}

func BytesToDecimalType(data []byte) *DecimalType {
	str := BytesToString(data)
	separatorIdx := strings.IndexByte(str, '.')
	p, _ := strconv.Atoi(str[:separatorIdx])
	//s, _ := strconv.Atoi(str[separatorIdx+1:])
	s, _ := strconv.Atoi(strings.TrimRight(str[separatorIdx+1:], "0"))
	return &DecimalType{
		precision: int32(p),
		scale:     int32(s),
	}
}

func DecimalTypeToBytes(v *DecimalType) []byte {
	negativeSign := ""
	if v.negativeSignFlag {
		negativeSign = "-"
	}
	s := fmt.Sprintf("%s%d.%d", negativeSign, v.precision, v.scale)
	return StringToBytes(s)
}

func BytesToDoubleType(data []byte) *DoubleType {
	return &DoubleType{
		Value: BytesToFloat64(data),
	}
}
func DoubleTypeToBytes(v *DoubleType) []byte {
	return Float64ToBytes(v.Value)
}

func BytesToSmallintType(data []byte) *SmallintType {
	return &SmallintType{
		Value: BytesToInt16(data),
	}
}

func SmallintTypeToBytes(v *SmallintType) []byte {
	return Int16ToBytes(v.Value)
}

func BytesToTimestampType(data []byte) *TimestampType {
	return &TimestampType{
		Value: BytesToInt64(data),
	}
}

func BytesToDateTimeType(data []byte) *DateTimeType {
	return &DateTimeType{
		Value: BytesToInt64(data),
	}
}

func TimestampTypeToBytes(v *TimestampType) []byte {
	return Int64ToBytes(v.Value)
}

func DateTimeTypeToBytes(v *DateTimeType) []byte {
	return Int64ToBytes(v.Value)
}

func BytesToTinyintType(data []byte) *TinyintType {
	return &TinyintType{
		Value: BytesToInt8(data),
	}
}

func TinyintTypeToBytes(v *TinyintType) []byte {
	return Int8ToBytes(v.Value)
}

func BytesToCharType(data []byte, maxLen uint16) *CharType {
	return &CharType{
		MaxLength: maxLen,
		Value:     BytesToString(data),
	}
}

func CharTypeToBytes(v *CharType) []byte {
	return StringToBytes(v.Value)
}

func BytesToVarcharType(data []byte, maxLen int32) *VarcharType {
	return &VarcharType{
		MaxLength: maxLen,
		Value:     BytesToString(data),
	}
}

func VarcharTypeToBytes(v *VarcharType) []byte {
	return StringToBytes(v.Value)
}

func ArrayToBytes(array *ArrayType) []byte {
	// TODO A more efficient way？
	// array bytes: <4 bytes element_count><1 byte element_type><len1><data1><len2><data2>...

	// encode header
	var outputBytes []byte
	outputBytes = append(outputBytes, Int32ToBytes(int32(array.Length))...)
	outputBytes = append(outputBytes, StandardTypeToCode(array.Type))
	if array.Length == 0 {
		return outputBytes
	}
	// encode elements
	for _, e := range array.Value {
		valBytes := e.ToBytes()
		valLen := int32(len(valBytes))
		outputBytes = append(outputBytes, Int32ToBytes(valLen)...)
		outputBytes = append(outputBytes, valBytes...)
	}
	return outputBytes
}

func BytesToArray(data []byte) *ArrayType {
	// 1. Fixed width type:
	// array bytes: <4 bytes element_count><1 byte element_type><element_data1><element_data2>...
	// 2. Variable width type:
	// array bytes: <4 bytes element_count><1 byte element_type><len1><data1><len2><data2>...

	resultArray := ArrayType{
		Type:   EMPTY,
		Value:  []Type{},
		Length: 0,
	}

	// decode header
	elementCount := BytesToInt32(data[0:4])
	elementType := StandardTypeFromCode(data[4])
	resultArray.Length = int(elementCount)
	resultArray.Type = elementType
	if len(data) == 5 {
		return &resultArray
	}

	// decode elements
	var startIdx = 5
	var end = len(data)
	for startIdx+4 <= end {
		elementSize := int(BytesToInt32(data[startIdx : startIdx+4]))
		elementBytes := data[startIdx+4 : startIdx+4+elementSize]
		element, err := ByteToStandardType(string(elementType), elementBytes)
		if err != nil {
			panic(err)
		}
		resultArray.Value = append(resultArray.Value, element)

		// forward
		startIdx = startIdx + 4 + elementSize
	}

	//
	return &resultArray
}

/*
Conversion of Primary Types to Bytes
*/
func byteToBytes(b byte) []byte {
	return []byte{
		b,
	}
}

func BytesToByte(b []byte) byte {
	return b[0]
}

func boolToBytes(v bool) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, v)
	return bytesBuffer.Bytes()
}

func BytesToBool(b []byte) bool {
	bytesBuffer := bytes.NewBuffer(b)
	var x bool
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

func Int8ToBytes(n int8) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToInt8(b []byte) int8 {
	bytesBuffer := bytes.NewBuffer(b)
	var x int8
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

func Int16ToBytes(n int16) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToInt16(b []byte) int16 {
	bytesBuffer := bytes.NewBuffer(b)
	var x int16
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

func Int32ToBytes(n int32) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToInt32(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

func Int64ToBytes(n int64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToInt64(b []byte) int64 {
	bytesBuffer := bytes.NewBuffer(b)
	var x int64
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

func Float32ToBytes(n float32) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToFloat32(b []byte) float32 {
	bytesBuffer := bytes.NewBuffer(b)
	var x float32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

func Float64ToBytes(n float64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToFloat64(b []byte) float64 {
	bytesBuffer := bytes.NewBuffer(b)
	var x float64
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

/*
Aggregation state
*/
func AggregationStateToBytes(state AbstractAggregationStateType) []byte {

	// bytes: <8 bytes long (count) value><1 byte state_type><state_value>

	// encode header
	var outputBytes []byte
	outputBytes = append(outputBytes, Int64ToBytes(int64(state.GetLongValue()))...)
	outputBytes = append(outputBytes, StateTypeToCode(state.GetStateType()))
	var valBytes []byte
	switch state.GetStateType() {
	case State_Double:
		valBytes = Float64ToBytes(state.GetStateValue().(float64))
		outputBytes = append(outputBytes, valBytes...)
	case State_Boolean:
		var boolValue *bool
		if state.GetStateValue() != nil {
			boolValue = state.GetStateValue().(*bool)
		}
		if boolValue != nil {
			valBytes = boolToBytes(*boolValue)
			outputBytes = append(outputBytes, valBytes...)
		}
	case State_Value:
		value := state.GetStateValue()
		outputBytes = append(outputBytes, StandardTypeToCode(value.(Type).GetStandardType()))
		valBytes = value.(Type).ToBytes()
		outputBytes = append(outputBytes, valBytes...)
	case State_Long:
		var longValue *int64
		if state.GetStateValue() != nil {
			longValue = state.GetStateValue().(*int64)
		}
		if longValue != nil {
			valBytes = Int64ToBytes(*(state.GetStateValue().(*int64)))
			outputBytes = append(outputBytes, valBytes...)
		}
	case State_Pair:
		value := state.GetStateValue()
		valBytes = PairToBytes(value.(*Pair))
		outputBytes = append(outputBytes, valBytes...)
	case State_PairArray:
		value := state.GetStateValue()
		valBytes = PairArrayToBytes(value.(*PairArray))
		outputBytes = append(outputBytes, valBytes...)
	case State_StandardValueArray:
		value := state.GetStateValue()
		valBytes = StandardValueArrayToBytes(value.(StandardValueArray))
		outputBytes = append(outputBytes, valBytes...)
	default:
		panic(errors.Newf(errors.UnknownType, "AggregationStateToBytes typeStr unKnown %s", state.GetStateType()))
	}

	return outputBytes
}

func BytesToAggregationState(data []byte) AbstractAggregationStateType {

	// bytes: <8 bytes long (count) value><1 byte state_type><state_value>

	stateCountValue := BytesToInt64(data[0:8])
	stateType := StateTypeFromCode(data[8])

	switch stateType {
	case State_Double:
		state := LongAndDoubleState{}
		value := BytesToFloat64(data[9:])
		state.Long = stateCountValue
		state.Double = value
		return &state
	case State_Boolean:
		state := LongAndBooleanState{}
		value := BytesToBool(data[9:])
		state.Long = stateCountValue
		state.Boolean = &value
		return &state
	case State_Value:
		// <8 bytes state long count><1 byte stage type><1 byte standard type code><data bytes>
		state := LongAndValueState{}
		code := data[9]
		standardType := StandardTypeFromCode(code)
		valueBytes := data[10:]
		value, err := ByteToStandardType(string(standardType), valueBytes)
		if err != nil {
			panic(err)
		}
		state.Long = stateCountValue
		state.Value = value
		return &state
	case State_Long:
		state := LongAndBooleanState{}
		value := BytesToInt64(data[9:])
		state.Long = stateCountValue
		state.Long = value
		return &state
	case State_Pair:
		state := LongAndPairState{}
		valueBytes := data[9:]
		pair := BytesToPair(valueBytes)
		state.Long = stateCountValue
		state.Pair = pair
		return &state
	case State_PairArray:
		state := LongAndPairArrayState{}
		valueBytes := data[9:]
		pairArray := BytesToPairArray(valueBytes)
		state.Long = stateCountValue
		state.Array = *pairArray
		return &state
	case State_StandardValueArray:
		state := LongAndArrayState{}
		valueBytes := data[9:]
		array := BytesToStandardValueArray(valueBytes)
		state.Long = stateCountValue
		state.Array = *array
		return &state
	default:
		panic(errors.Newf(errors.UnknownType, "AggregationStateToBytes typeStr unKnown %s", stateType))
	}

}

func ByteToStandardType(typeStr string, data []byte) (Type, errors.Error) {
	switch typeStr {
	case string(BIGINT):
		return BytesToBigintType(data), nil
	case string(INTEGER):
		return BytesToIntegerType(data), nil
	case string(SMALLINT):
		return BytesToSmallintType(data), nil
	case string(TINYINT):
		return BytesToTinyintType(data), nil
	case string(BOOLEAN):
		return BytesToBooleanType(data), nil
	case string(DATE):
		return BytesToDateType(data), nil
	case string(DECIMAL):
		return BytesToDecimalType(data), nil
	case string(DOUBLE):
		return BytesToDoubleType(data), nil
	case string(TIMESTAMP):
		return BytesToTimestampType(data), nil
	case string(TIME):
		return BytesToTimeType(data), nil
	case string(VARCHAR):
		return BytesToVarcharType(data, 1024), nil
	case string(CHAR):
		return BytesToCharType(data, 1024), nil
	case string(EMPTY):
		return &EmptyType{}, nil
	case string(ARRAY):
		return BytesToArray(data), nil
	case string(JSON):
		//temp
		v := BytesToVarcharType(data, 1024)
		return &JsonType{Value: v.Value, MaxLength: v.MaxLength}, nil
	case string(AGGREGATION_STATE_TYPE):
		return BytesToAggregationState(data), nil
	case string(DATETIME):
		return BytesToDateTimeType(data), nil
	default:
		return nil, errors.Newf(errors.GenericUnknownError, "ByteToStandardType typeStr unKnown %+v", typeStr)
	}
}

// StringBytesToStandardType string bytes to StandardType
func StringBytesToStandardType(valueType StandardType, stringBytes []byte) (Type, errors.Error) {
	strValue := BytesToString(stringBytes)
	switch valueType {
	case BIGINT:
		v, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			panic(err)
		}
		return &BigintType{
			Value: v,
		}, nil
	case INTEGER:
		v, err := strconv.Atoi(strValue)
		if err != nil {
			panic(err)
		}
		return &IntegerType{
			Value: int32(v),
		}, nil
	case VARCHAR:
		return &VarcharType{
			Value: string(strValue),
		}, nil
	case SMALLINT:
		v, err := strconv.ParseInt(strValue, 10, 16)
		if err != nil {
			panic(err)
		}
		return &SmallintType{
			Value: int16(v),
		}, nil
	case TINYINT:
		v, err := strconv.ParseInt(strValue, 10, 8)
		if err != nil {
			panic(err)
		}
		return &TinyintType{
			Value: int8(v),
		}, nil
	case BOOLEAN:
		var v bool
		switch strValue {
		case "true":
			v = true
		case "false":
			v = false
		default:
			return nil, errors.Newf(errors.GenericUnknownError, "StringBytesToStandardType boolean str is not true or false %s", strValue)
		}
		return &BooleanType{
			Value: v,
		}, nil
	case DATE:
		v, err := strconv.ParseInt(strValue, 10, 32)
		if err != nil {
			panic(err)
		}
		return &DateType{
			Value: int32(v),
		}, nil
	case DECIMAL:
		separatorIdx := strings.IndexByte(strValue, '.')
		precision, err := strconv.ParseInt(strValue[:separatorIdx], 10, 32)
		if err != nil {
			panic(err)
		}
		scale, err := strconv.ParseInt(strValue[separatorIdx+1:], 10, 32)
		if err != nil {
			panic(err)
		}
		var negativeSignFlag bool
		if precision == 0 && strings.IndexByte(strValue, '-') != -1 {
			negativeSignFlag = true
		}
		return &DecimalType{
			negativeSignFlag: negativeSignFlag,
			precision:        int32(precision),
			scale:            int32(scale),
		}, nil
	case DOUBLE:
		v, err := strconv.ParseFloat(strValue, 64)
		if err != nil {
			panic(err)
		}
		return &DoubleType{
			Value: v,
		}, nil
	case TIMESTAMP:
		v, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			panic(err)
		}
		return &TimestampType{
			Value: int64(v),
		}, nil
	case TIME:
		v, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			panic(err)
		}
		return &TimeType{
			Value: int64(v),
		}, nil
	case CHAR:
		return &CharType{
			Value: strValue,
		}, nil
	case JSON:
		//temp
		return &JsonType{
			Value: string(strValue),
		}, nil
	case DATETIME:
		v, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			panic(err)
		}
		return &DateTimeType{
			Value: int64(v),
		}, nil
	default:
		return nil, errors.Newf(errors.GenericUnknownError, "StringBytesToStandardType typeStr unKnown %+v", valueType)
	}
}

var NumberUpTypeConversionMap map[StandardType]UpTypeConversion = map[StandardType]UpTypeConversion{
	TINYINT:  {0, CastToSmallInt},
	SMALLINT: {1, CastToSmallInt},
	INTEGER:  {2, CastToInteger},
	BIGINT:   {3, CastToBigint},
	DOUBLE:   {4, CastToDouble},
	DECIMAL:  {5, CastToDecimal},
}

type UpTypeConversion struct {
	UpTypeLevel          int
	UpTypeConversionFunc func(Type) Type
}

func StringToDecimal(value string) *DecimalType {
	decimalType := new(DecimalType)
	err := setValueType(value, decimalType)
	if err != nil {
		panic(err)
	}
	return decimalType
}

func CastToSmallInt(value Type) Type {
	var smallIntType *SmallintType = new(SmallintType)
	switch value := value.(type) {
	case *TinyintType:
		smallIntType.Value = int16(value.Value)
	case *SmallintType:
		smallIntType = value
	default:
		panic("not support Cast Type " + value.GetStandardType())
	}
	return smallIntType
}

func CastToInteger(value Type) Type {
	var integerType *IntegerType = new(IntegerType)
	switch value := value.(type) {
	case *SmallintType:
		integerType.Value = int32(value.Value)
	case *TinyintType:
		integerType.Value = int32(value.Value)
	case *IntegerType:
		integerType = value
	default:
		panic("not support Cast Type " + value.GetStandardType())
	}
	return integerType
}

func CastToBigint(value Type) Type {
	var bigintType *BigintType = new(BigintType)
	switch value := value.(type) {
	case *IntegerType:
		bigintType.Value = int64(value.Value)
	case *SmallintType:
		bigintType.Value = int64(value.Value)
	case *TinyintType:
		bigintType.Value = int64(value.Value)
	case *BigintType:
		bigintType = value
	default:
		panic("not support Cast Type " + value.GetStandardType())
	}
	return bigintType
}

func CastToDouble(value Type) Type {
	var doubleType *DoubleType = new(DoubleType)
	switch value := value.(type) {
	case *BigintType:
		doubleType.Value = float64(value.Value)
	case *IntegerType:
		doubleType.Value = float64(value.Value)
	case *SmallintType:
		doubleType.Value = float64(value.Value)
	case *TinyintType:
		doubleType.Value = float64(value.Value)
	case *DoubleType:
		doubleType = value
	default:
		panic("not support Cast Type " + value.GetStandardType())
	}
	return doubleType
}

func CastToDecimal(value Type) Type {
	var decimalType *DecimalType = new(DecimalType)
	switch value := value.(type) {
	case *DoubleType:
		setValueType(value.Value, decimalType)
	case *BigintType:
		if value.Value <= math.MaxInt32 {
			decimalType.precision = int32(value.Value)
		} else {
			panic("not support Cast Type " + value.GetStandardType())
		}
	case *IntegerType:
		decimalType.precision = int32(value.Value)
	case *SmallintType:
		decimalType.precision = int32(value.Value)
	case *TinyintType:
		decimalType.precision = int32(value.Value)
	case *DecimalType:
		decimalType = value
	default:
		panic("not support Cast Type " + value.GetStandardType())
	}
	return decimalType
}

func StandardTypeCompare(valueType StandardType, left Type, right Type) (float64, errors.Error) {
	if left.GetStandardType() == EMPTY && right.GetStandardType() == EMPTY {
		return 0, nil
	} else if left.GetStandardType() != EMPTY && right.GetStandardType() == EMPTY {
		return 1, nil
	} else if left.GetStandardType() == EMPTY && right.GetStandardType() != EMPTY {
		return -1, nil
	}
	if left.GetStandardType() != right.GetStandardType() {
		leftUpTypeConversion, leftUpTypeExist := NumberUpTypeConversionMap[left.GetStandardType()]
		rightUpTypeConversion, rightUpTypeExist := NumberUpTypeConversionMap[right.GetStandardType()]
		if leftUpTypeExist && rightUpTypeExist {
			if leftUpTypeConversion.UpTypeLevel > rightUpTypeConversion.UpTypeLevel {
				valueType = left.GetStandardType()
				right = leftUpTypeConversion.UpTypeConversionFunc(right)
			} else {
				valueType = right.GetStandardType()
				left = rightUpTypeConversion.UpTypeConversionFunc(left)
			}
		}
	}
	return standardTypeCompare(valueType, left, right)
}

func standardTypeCompare(valueType StandardType, left Type, right Type) (float64, errors.Error) {
	switch valueType {
	case BIGINT:
		var l int64
		var r int64
		if lVal, ok := left.(*BigintType); ok {
			l = lVal.Value
		} else if lVal, ok := left.(*IntegerType); ok {
			l = int64(lVal.Value)
		}
		if rVal, ok := right.(*BigintType); ok {
			r = rVal.Value
		} else if rVal, ok := right.(*IntegerType); ok {
			r = int64(rVal.Value)
		}
		return float64(l - r), nil
	case INTEGER:
		var l int64
		var r int64
		if lVal, ok := left.(*BigintType); ok {
			l = lVal.Value
		} else if lVal, ok := left.(*IntegerType); ok {
			l = int64(lVal.Value)
		}
		if rVal, ok := right.(*BigintType); ok {
			r = rVal.Value
		} else if rVal, ok := right.(*IntegerType); ok {
			r = int64(rVal.Value)
		}
		return float64(l - r), nil
	case SMALLINT:
		l := left.(*SmallintType)
		r := right.(*SmallintType)
		return float64(l.Value - r.Value), nil
	case TINYINT:
		l := left.(*TinyintType)
		r := right.(*TinyintType)
		return float64(l.Value - r.Value), nil
	case BOOLEAN:
		l := left.(*BooleanType)
		r := right.(*BooleanType)
		if l.Value == r.Value {
			return 0, nil
		}
		return -1, nil
	case DATE:
		var l int32
		var r int32
		if lVal, ok := left.(*DateType); ok {
			l = lVal.Value
		} else {
			l = left.CastTo(DATE).(*DateType).Value
		}
		if rVal, ok := right.(*DateType); ok {
			r = rVal.Value
		} else {
			r = right.CastTo(DATE).(*DateType).Value
		}
		return float64(l - r), nil
	case DOUBLE:
		var l float64
		var r float64
		if lVal, ok := left.(*DoubleType); ok {
			l = lVal.Value
		} else {
			d := left.CastTo(DOUBLE)
			l = d.(*DoubleType).Value
		}
		if rVal, ok := right.(*DoubleType); ok {
			r = rVal.Value
		} else {
			d := right.CastTo(DOUBLE)
			r = d.(*DoubleType).Value
		}
		return l - r, nil
	case TIMESTAMP:
		var l int64
		var r int64
		if lVal, ok := left.(*TimestampType); ok {
			l = lVal.Value
		} else {
			l = left.CastTo(TIMESTAMP).(*TimestampType).Value
		}
		if rVal, ok := right.(*TimestampType); ok {
			r = rVal.Value
		} else {
			r = right.CastTo(TIMESTAMP).(*TimestampType).Value
		}
		return float64(l - r), nil
	case TIME:
		var l int64
		var r int64
		if lVal, ok := left.(*TimeType); ok {
			l = lVal.Value
		} else {
			l = left.CastTo(TIME).(*TimeType).Value
		}
		if rVal, ok := right.(*TimeType); ok {
			r = rVal.Value
		} else {
			r = right.CastTo(TIME).(*TimeType).Value
		}
		return float64(l - r), nil
	case VARCHAR, CHAR:
		var l string
		var r string
		if lVal, ok := left.(*VarcharType); ok {
			l = lVal.Value
		} else if lVal, ok := left.(*CharType); ok {
			l = lVal.Value
		} else {
			l = left.CastTo(VARCHAR).(*VarcharType).Value
		}

		if rVal, ok := right.(*VarcharType); ok {
			r = rVal.Value
		} else if rVal, ok := right.(*CharType); ok {
			r = rVal.Value
		} else {
			r = right.CastTo(VARCHAR).(*VarcharType).Value
		}
		return float64(strings.Compare(l, r)), nil
	case DECIMAL:
		l := left.(*DecimalType)
		r := right.(*DecimalType)
		if l.negativeSignFlag == r.negativeSignFlag {
			var sign int32 = 1
			if l.negativeSignFlag {
				sign = -1
			}
			if l.precision == r.precision {
				return float64((l.scale - r.scale) * sign), nil
			} else {
				return float64((l.precision - r.precision) * sign), nil
			}
		} else {
			if l.negativeSignFlag {
				if r.precision != 0 {
					return float64(0 - r.precision), nil
				} else {
					return float64(0 - r.scale), nil
				}
			} else {
				if l.precision != 0 {
					return float64(l.precision), nil
				} else {
					return float64(l.scale), nil
				}

			}
		}
	case ARRAY:
		return 0, nil
	case JSON:
		//temp
		l := left.(*JsonType)
		r := right.(*JsonType)
		return float64(strings.Compare(l.Value, r.Value)), nil
	case DATETIME:
		var l int64
		var r int64
		if lVal, ok := left.(*TimestampType); ok {
			l = lVal.Value
		} else {
			l = left.CastTo(TIMESTAMP).(*TimestampType).Value
		}
		if rVal, ok := right.(*TimestampType); ok {
			r = rVal.Value
		} else {
			r = right.CastTo(TIMESTAMP).(*TimestampType).Value
		}
		return float64(l - r), nil
	default:
		return 0, errors.Newf(errors.GenericUnknownError, "StandardTypeCompare type  unKnown %+v", valueType)
	}
}

func GetBaseValue(typeValue Type) (any, errors.Error) {
	switch value := typeValue.(type) {
	case *BooleanType:
		return value.Value, nil
	case *BigintType:
		return value.Value, nil
	case *IntegerType:
		return value.Value, nil
	case *TinyintType:
		return value.Value, nil
	case *SmallintType:
		return value.Value, nil
	case *VarcharType:
		return value.Value, nil
	case *CharType:
		return value.Value, nil
	case *DoubleType:
		return value.Value, nil
	case *TimestampType:
		return value.Value, nil
	case *TimeType:
		return value.Value, nil
	case *DateType:
		return value.Value, nil
	case *DecimalType:
		str := fmt.Sprintf("%d.%d", value.precision, value.scale)
		v, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return nil, errors.Newf(errors.GenericUnknownError, "GetBaseValue decimal is illegal %+v", value)
		}
		return v, nil
	case *ArrayType:
		return ArrayToBytes(value), nil
	case *EmptyType:
		return nil, nil
	case *JsonType:
		//temp
		return value.Value, nil
	default:
		return nil, errors.Newf(errors.GenericUnknownError, "GetBaseType not support type %s", value.GetStandardType())
	}
}

func ConvertValueTypeByStandardType(t StandardType, value any) (Type, errors.Error) {
	switch t {
	case TINYINT:
		return CastType(new(TinyintType), value)
	case SMALLINT:
		return CastType(new(SmallintType), value)
	case INTEGER:
		return CastType(new(IntegerType), value)
	case BIGINT:
		return CastType(new(BigintType), value)
	case BOOLEAN:
		return CastType(new(BooleanType), value)
	case DOUBLE:
		return CastType(new(DoubleType), value)
	case VARCHAR:
		return CastType(new(VarcharType), value)
	case CHAR:
		return CastType(new(CharType), value)
	case TIMESTAMP:
		return CastType(new(TimestampType), value)
	case DATE:
		return CastType(new(DateType), value)
	case TIME:
		return CastType(new(TimeType), value)
	case DECIMAL:
		return CastType(new(DecimalType), value)
	case ARRAY:
		return CastType(new(ArrayType), value)
	case EMPTY:
		return GetValueTypeByBaseType(value)
	case JSON:
		return CastType(new(JsonType), value)
	case DATETIME:
		return CastType(new(DateTimeType), value)
	default:
		return nil, errors.Newf(errors.GenericUnknownError, "ConvertValueTypeByStandardType undefine type %s", t)
	}
}

func GetValueTypeByBaseType(baseValue any) (Type, errors.Error) {
	switch value := baseValue.(type) {
	case int8:
		return &TinyintType{Value: int8(value)}, nil
	case int16:
		return &SmallintType{Value: int16(value)}, nil
	case int:
		return &IntegerType{Value: int32(value)}, nil
	case int32:
		return &IntegerType{Value: value}, nil
	case int64:
		return &BigintType{Value: value}, nil
	case float64:
		return &DoubleType{Value: value}, nil
	case float32:
		return &DoubleType{Value: float64(value)}, nil
	case string:
		return &VarcharType{Value: value}, nil
	default:
		return nil, errors.Newf(errors.GenericUnknownError, "not support type %s", utils.GetTypeName(value))
	}
}

func setValueType(src any, dest any) errors.Error {
	var srcStr string
	switch src := src.(type) {
	case string:
		srcStr = src
	case float64:
		srcStr = strconv.FormatFloat(src, 'f', -1, 64)
	case int64:
		srcStr = strconv.FormatInt(src, 10)
	case int32:
		srcStr = strconv.FormatInt(int64(src), 10)
	case bool:
		srcStr = strconv.FormatBool(src)
	case *bool:
		srcStr = strconv.FormatBool(*src)
	case *string:
		srcStr = *src
	case *float64:
		srcStr = strconv.FormatFloat(*src, 'f', -1, 64)
	case *int64:
		srcStr = strconv.FormatInt(*src, 10)
	case *int32:
		srcStr = strconv.FormatInt(int64(*src), 10)
	case []byte:
		srcStr = string(src)
	case *[]byte:
		srcStr = string(*src)
	case time.Time:
		var builder strings.Builder
		if src.Year() >= 1970 {
			builder.WriteString(fmt.Sprintf("%d", src.Year()))
			builder.WriteString("-")
			builder.WriteString(fmt.Sprintf("%02d", src.Month()))
			builder.WriteString("-")
			builder.WriteString(fmt.Sprintf("%02d", src.Day()))
			builder.WriteString(" ")
		}
		builder.WriteString(fmt.Sprintf("%02d", src.Hour()))
		builder.WriteString(":")
		builder.WriteString(fmt.Sprintf("%02d", src.Minute()))
		builder.WriteString(":")
		builder.WriteString(fmt.Sprintf("%02d", src.Second()))
		if src.Nanosecond() > 0 {
			builder.WriteString(".")
			builder.WriteString(strconv.Itoa(src.Nanosecond()))
		}
		srcStr = builder.String()
		fmt.Printf("setValueType time src:[%+v], srcStr:[%s], dest:[%+v]\n", src, srcStr, dest)
	default:
		return errors.Newf(errors.GenericUnknownError, "unspecified type %s", utils.GetTypeName(src))
	}
	var err error
	switch dest := dest.(type) {
	case *BooleanType:
		dest.Value, err = strconv.ParseBool(srcStr)
		if err != nil {
			switch src := src.(type) {
			case float64, int64:
				if src != 0 {
					dest.Value = true
				} else {
					dest.Value = false
				}
			default:
				return errors.Newf(errors.GenericUnknownError, "unspecified type %s", reflect.ValueOf(src).Kind().String())
			}
		}
	case *TinyintType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		dest.Value = int8(intValue)
	case *SmallintType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		dest.Value = int16(intValue)
	case *IntegerType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		dest.Value = int32(intValue)
	case *BigintType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		dest.Value = intValue
	case *DoubleType:
		dest.Value, err = strconv.ParseFloat(srcStr, 64)
	case *VarcharType:
		dest.Value = srcStr
	case *CharType:
		dest.Value = srcStr
	case *TimestampType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		if err != nil {
			timeMillis, err1 := utils.ParseTimeStrToMilliSeconds(srcStr)
			if err1 != nil {
				fmt.Printf("TimestampType error:[%+v] \n", err1)
				err1 = errors.Newf(errors.GenericUnknownError, "not support type %s", utils.GetTypeName(dest))
			} else {
				intValue = timeMillis
			}
			err = err1
		}
		dest.Value = intValue
	case *TimeType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		if err != nil {
			timeMillis, err1 := utils.ParseTimeStrToMilliSeconds(srcStr)
			if err1 != nil {
				fmt.Printf("TimeType error:[%+v] \n", err1)
				err1 = errors.Newf(errors.GenericUnknownError, "not support type %s", utils.GetTypeName(dest))
			} else {
				intValue = timeMillis
			}
			err = err1
		}
		dest.Value = intValue
	case *DateType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		if err != nil {
			days, err1 := utils.ParseDateStrToDays(srcStr)
			if err1 != nil {
				fmt.Printf("DateType error:[%+v] \n", err1)
				err1 = errors.Newf(errors.GenericUnknownError, "not support type %s", utils.GetTypeName(dest))
			} else {
				intValue = int64(days)
			}
			err = err1
		}
		dest.Value = int32(intValue)
	case *DecimalType:
		separatorIdx := strings.IndexByte(srcStr, '.')
		if separatorIdx == -1 {
			p, _ := strconv.Atoi(srcStr)
			dest.precision = int32(p)
			dest.scale = 0
		} else {
			p, _ := strconv.Atoi(srcStr[:separatorIdx])
			s, _ := strconv.Atoi(strings.TrimRight(srcStr[separatorIdx+1:], "0"))
			if p == 0 && strings.IndexByte(srcStr, '-') != -1 {
				dest.negativeSignFlag = true
			}
			dest.precision = int32(p)
			dest.scale = int32(s)
		}
	case *ArrayType:
		arrayValue := BytesToArray(src.([]byte))
		dest.Type = arrayValue.Type
		dest.Length = arrayValue.Length
		dest.Value = arrayValue.Value
	case *JsonType:
		//temp
		dest.Value = srcStr
	case *DateTimeType:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		if err != nil {
			timeMillis, err1 := utils.ParseTimeStrToMilliSeconds(srcStr)
			if err1 != nil {
				fmt.Printf("DateTimeType error:[%+v] \n", err1)
				err1 = errors.Newf(errors.GenericUnknownError, "not support type %s", utils.GetTypeName(dest))
			} else {
				intValue = timeMillis
			}
			err = err1
		}
		dest.Value = intValue
	default:
		err = errors.Newf(errors.GenericUnknownError, "not support type %s", utils.GetTypeName(dest))
	}
	if err != nil {
		return errors.New(errors.UnknownType, err.Error())
	}
	return nil
}

func CastType(retValue Type, value any) (Type, errors.Error) {
	var err errors.Error
	if value != nil {
		err = setValueType(value, retValue)
	}
	return retValue, err
}

func ConvertBaseValueByStandardType(t StandardType, value any) (any, errors.Error) {
	switch t {
	case TINYINT:
		return CastBaseType[int8](value)
	case SMALLINT:
		return CastBaseType[int16](value)
	case INTEGER:
		return CastBaseType[int32](value)
	case BIGINT:
		return CastBaseType[int64](value)
	case BOOLEAN:
		return CastBaseType[bool](value)
	case DOUBLE:
		return CastBaseType[float64](value)
	case VARCHAR:
		return CastBaseType[string](value)
	case CHAR:
		return CastBaseType[string](value)
	case TIMESTAMP:
		return CastBaseType[int64](value)
	case TIME:
		return CastBaseType[int64](value)
	case DATE:
		return CastBaseType[int32](value)
	case DECIMAL:
		return CastBaseType[float64](value)
	case ARRAY:
		return nil, errors.Newf(errors.NotSupported, "ConvertBaseValueByStandardType not support array type %s", t)
	case DATETIME:
		return CastBaseType[int64](value)
	default:
		return nil, errors.Newf(errors.GenericUnknownError, "undefine type %s", t)
	}
}

func setValue(src any, dest any) errors.Error {
	var srcStr string
	switch src := src.(type) {
	case bool:
		srcStr = strconv.FormatBool(src)
	case float64:
		srcStr = strconv.FormatFloat(src, 'f', -1, 64)
	case string:
		srcStr = src
	case int64:
		srcStr = strconv.FormatInt(src, 10)
	case int:
		srcStr = strconv.FormatInt(int64(src), 10)
	case int32:
		srcStr = strconv.FormatInt(int64(src), 10)
	case []byte:
		srcStr = string(src)
	default:
		return errors.Newf(errors.GenericUnknownError, "unspecified type %s", reflect.ValueOf(src).Kind().String())
	}
	var err error
	switch dest := dest.(type) {
	case *bool:
		*dest, err = strconv.ParseBool(srcStr)
		if err != nil {
			switch src := src.(type) {
			case float64, int64:
				if src != 0 {
					*dest = true
				} else {
					*dest = false
				}
			default:
				return errors.Newf(errors.GenericUnknownError, "unspecified type %s", reflect.ValueOf(src).Kind().String())
			}
		}
	case *int32:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		*dest = int32(intValue)
	case *int64:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		*dest = intValue
	case *int:
		var intValue int64
		intValue, err = strconv.ParseInt(srcStr, 10, 64)
		*dest = int(intValue)
	case *float64:
		*dest, err = strconv.ParseFloat(srcStr, 64)
	case *string:
		*dest = srcStr
	case *ArrayType:
		// basicArrayBytes := ArrayBytesToBasicArrayBytes(src.([]byte))
		// arrayValue, err := json.Marshal(basicArrayBytes)
		// if err != nil {
		// 	return errors.New(errors.UnknownType, err.Error())
		// }
		// *dest = string(arrayValue)
	default:
	}
	if err != nil {
		return errors.New(errors.UnknownType, err.Error())
	}
	return nil
}

func CastBaseType[T any](value any) (T, errors.Error) {
	var retValue T
	var err errors.Error
	if value != nil {
		err = setValue(value, &retValue)
	}
	return retValue, err
}

func IsDecimalStandardType(typeName string) bool {
	name := strings.ToLower(typeName)
	switch StandardType(name) {
	case DECIMAL, DOUBLE:
		return true
	default:
		return false
	}
}

func FormatTimeToString(timeValue int64, format string) string {
	tm := time.Unix(timeValue, 0)
	return tm.Format(format)
}

func BytesToChar(b []byte) byte {
	return b[0]
}

func CharToBytes(b byte) []byte {
	return []byte{
		b,
	}
}

// BytesToString converts byte slice to string.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts string to byte slice.
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
