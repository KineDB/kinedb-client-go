package utils

import (
	"fmt"
	"reflect"

	"github.com/industry-tenebris/kinedb-goclient/common/errors"
)

func GetBasicTypeMashalValue(originType reflect.Kind, v interface{}) (string, errors.Error) {
	switch originType {
	case reflect.Int32:
		return fmt.Sprintf("%v", v), nil
	case reflect.String:
		return fmt.Sprintf("\"%v\"", v), nil
	case reflect.Int64:
		return fmt.Sprintf("%v", v), nil
	case reflect.Float32:
		return fmt.Sprintf("%v", v), nil
	case reflect.Float64:
		return fmt.Sprintf("%v", v), nil
	case reflect.Int:
		return fmt.Sprintf("%v", v), nil
	case reflect.Int8:
		return fmt.Sprintf("%v", v), nil
	case reflect.Int16:
		return fmt.Sprintf("%v", v), nil
	case reflect.Bool:
		return fmt.Sprintf("%v", v), nil
	default:
		return "", errors.Newf(errors.GenericUnknownError, "GetBasicTypeMashalValue type unKnown %+v", originType)
	}
}
