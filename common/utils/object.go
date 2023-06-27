package utils

import (
	"reflect"
)

func IntMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func NotNil(pointer any) bool {
	return !IsNil(pointer)
}

func IsNil(pointer any) bool {
	if pointer == nil {
		return true
	}
	v := reflect.ValueOf(pointer)
	if v.Kind() == reflect.Ptr {
		return v.IsNil()
	}
	return false
}

func GetTypeName(object any) string {
	if object != nil {
		return reflect.ValueOf(object).Type().String()
	}
	return "nil"
}
