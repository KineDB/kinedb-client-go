package utils

import (
	"reflect"
)

func IsArray(doc any) bool {
	if doc != nil {
		switch reflect.TypeOf(doc).Kind() {
		case reflect.Slice, reflect.Array:
			return true
		default:
			return false
		}
	}
	return false
}

func IsMap(doc any) bool {
	if doc != nil {
		switch reflect.TypeOf(doc).Kind() {
		case reflect.Map:
			return true
		default:
			return false
		}
	}
	return false
}

func IsCollection(collection any) bool {
	if collection != nil {
		switch reflect.TypeOf(collection).Kind() {
		case reflect.Slice, reflect.Array, reflect.Map:
			return true
		}
	}
	return false
}

func Contains(collection any, target any) bool {
	collectionValue := reflect.ValueOf(collection)
	switch reflect.TypeOf(collection).Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < collectionValue.Len(); i++ {
			if collectionValue.Index(i).Interface() == target {
				return true
			}
		}
	case reflect.Map:
		if collectionValue.MapIndex(reflect.ValueOf(target)).IsValid() {
			return true
		}
	}
	return false
}

func NotContains(collection any, target any) bool {
	return !Contains(collection, target)
}

func Equals(a any, b any) bool {
	if reflect.ValueOf(a).CanInt() && reflect.ValueOf(b).CanInt() {
		return reflect.ValueOf(a).Int() == reflect.ValueOf(b).Int()
	}
	return a == b
}

func NotEqual(a any, b any) bool {
	return !Equals(a, b)
}
