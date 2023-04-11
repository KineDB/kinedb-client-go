package types

import (
	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

// Standard Value Array

type StandardValueArray []Type

func (arr StandardValueArray) Len() int {
	return len(arr)
}
func (arr StandardValueArray) Less(i, j int) bool {
	switch arr[i].GetStandardType() {
	case BIGINT:
		iFirst := arr[i].(*BigintType)
		jFirst := arr[j].(*BigintType)
		return iFirst.Value < jFirst.Value
	case INTEGER:
		iFirst := arr[i].(*IntegerType)
		jFirst := arr[j].(*IntegerType)
		return iFirst.Value < jFirst.Value
	case SMALLINT:
		iFirst := arr[i].(*SmallintType)
		jFirst := arr[j].(*SmallintType)
		return iFirst.Value < jFirst.Value
	case TINYINT:
		iFirst := arr[i].(*TinyintType)
		jFirst := arr[j].(*TinyintType)
		return iFirst.Value < jFirst.Value
	case DOUBLE:
		iFirst := arr[i].(*DoubleType)
		jFirst := arr[j].(*DoubleType)
		return iFirst.Value < jFirst.Value
	default:
		panic(errors.Newf(errors.NotSupported, "not support %s", arr[i].GetStandardType()))
	}
}
func (arr StandardValueArray) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

func StandardValueArrayToBytes(array StandardValueArray) []byte {
	// array bytes: <4 bytes element_count><len1><1 byte element_type><data1><len2><1 byte element_type><data2>...

	// encode header
	var outputBytes []byte
	length := len(array)
	outputBytes = append(outputBytes, Int32ToBytes(int32(length))...)
	if length == 0 {
		return outputBytes
	}
	// encode elements
	for _, e := range array {
		valBytes := e.ToBytes()
		valLen := int32(len(valBytes))
		outputBytes = append(outputBytes, Int32ToBytes(valLen)...)
		outputBytes = append(outputBytes, StandardTypeToCode(e.GetStandardType()))
		outputBytes = append(outputBytes, valBytes...)
	}
	return outputBytes
}

func BytesToStandardValueArray(data []byte) *StandardValueArray {
	array := StandardValueArray{}

	// decode header
	//elementCount := BytesToInt32(data[0:4])
	if len(data) == 4 {
		return &array
	}

	// decode elements
	var startIdx = 4
	var end = len(data)
	for startIdx+4 <= end {
		elementSize := int(BytesToInt32(data[startIdx : startIdx+4]))
		elementTypeByte := data[startIdx+4]
		elementType := StandardTypeFromCode(elementTypeByte)
		elementBytes := data[startIdx+5 : startIdx+5+elementSize]
		element, err := ByteToStandardType(string(elementType), elementBytes)
		if err != nil {
			panic(err)
		}
		array = append(array, element)

		// forward
		startIdx = startIdx + 5 + elementSize
	}

	//
	return &array
}

// Pair Array

type Pair struct {
	First  Type
	Second Type
}

func PairToBytes(p *Pair) []byte {
	// <len1 4><first type 1><First bytes><len2 4><second type 1><Second bytes>
	var outputBytes []byte
	valBytes1 := p.First.ToBytes()
	valLen1 := int32(len(valBytes1))
	outputBytes = append(outputBytes, Int32ToBytes(valLen1)...)
	outputBytes = append(outputBytes, StandardTypeToCode(p.First.GetStandardType()))
	outputBytes = append(outputBytes, valBytes1...)
	valBytes2 := p.Second.ToBytes()
	valLen2 := int32(len(valBytes2))
	outputBytes = append(outputBytes, Int32ToBytes(valLen2)...)
	outputBytes = append(outputBytes, StandardTypeToCode(p.Second.GetStandardType()))
	outputBytes = append(outputBytes, valBytes2...)
	return outputBytes
}

func BytesToPair(data []byte) *Pair {
	// <len1><First bytes><len2><Second bytes>
	pair := Pair{
		First:  &EmptyType{},
		Second: &EmptyType{},
	}

	// decode
	firstCount := BytesToInt32(data[0:4])
	if firstCount == 0 {
		return &pair
	}
	firstTypeByte := data[4]
	firstType := StandardTypeFromCode(firstTypeByte)
	firstBytes := data[5 : 5+firstCount]
	first, err := ByteToStandardType(string(firstType), firstBytes)
	if err != nil {
		panic(err)
	}
	pair.First = first

	secondStartIndex := 5 + firstCount
	secondCount := BytesToInt32(data[secondStartIndex : secondStartIndex+4])
	if secondCount == 0 {
		return &pair
	}
	secondTypeByte := data[secondStartIndex+4]
	secondType := StandardTypeFromCode(secondTypeByte)
	secondBytes := data[secondStartIndex+5 : secondStartIndex+5+secondCount]
	second, err := ByteToStandardType(string(secondType), secondBytes)
	if err != nil {
		panic(err)
	}
	pair.Second = second

	return &pair
}

type PairArray []Pair

func PairArrayToBytes(pa *PairArray) []byte {
	// array bytes: <4 bytes element_count><len1 4><first type 1><First bytes><len2 4><second type 1><Second bytes>...

	// encode header
	var outputBytes []byte
	length := len(*pa)
	if length == 0 {
		return outputBytes
	}
	outputBytes = append(outputBytes, Int32ToBytes(int32(length))...)

	// encode elements
	for _, e := range *pa {
		valBytes := PairToBytes(&e)
		outputBytes = append(outputBytes, valBytes...)
	}
	return outputBytes
}

func BytesToPairArray(data []byte) *PairArray {
	// array bytes: <4 bytes element_count><len1 4><first type 1><First bytes><len2 4><second type 1><Second bytes>...

	pairArray := PairArray{}

	// decode header
	elementCount := BytesToInt32(data[0:4])
	if elementCount == 0 {
		return &pairArray
	}

	// decode pairs
	var startIdx = 5
	var end = len(data)
	for startIdx+5 <= end {
		firstSize := int(BytesToInt32(data[startIdx : startIdx+4]))
		secondSize := int(BytesToInt32(data[startIdx+5+firstSize : startIdx+5+firstSize+4]))

		pairBytes := data[startIdx : startIdx+5+firstSize+5+secondSize]

		pair := BytesToPair(pairBytes)
		pairArray = append(pairArray, *pair)

		// forward
		startIdx = startIdx + 5 + firstSize + 5 + secondSize
	}

	//
	return &pairArray
}

func (arr PairArray) Len() int {
	return len(arr)
}
func (arr PairArray) Less(i, j int) bool {
	switch arr[i].First.GetStandardType() {
	case BIGINT:
		iFirst := arr[i].First.(*BigintType)
		jFirst := arr[j].First.(*BigintType)
		return iFirst.Value < jFirst.Value
	case INTEGER:
		iFirst := arr[i].First.(*IntegerType)
		jFirst := arr[j].First.(*IntegerType)
		return iFirst.Value < jFirst.Value
	case SMALLINT:
		iFirst := arr[i].First.(*SmallintType)
		jFirst := arr[j].First.(*SmallintType)
		return iFirst.Value < jFirst.Value
	case TINYINT:
		iFirst := arr[i].First.(*TinyintType)
		jFirst := arr[j].First.(*TinyintType)
		return iFirst.Value < jFirst.Value
	case DOUBLE:
		iFirst := arr[i].First.(*DoubleType)
		jFirst := arr[j].First.(*DoubleType)
		return iFirst.Value < jFirst.Value
	default:
		panic(errors.Newf(errors.NotSupported, "not support %s", arr[i].First.GetStandardType()))
	}
}
func (arr PairArray) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

// Reverse tool
func Reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
