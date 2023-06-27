package utils

import (
	"bytes"

	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func Struct2JsonString(v any) string {
	result, err := json.Marshal(v)
	if err != nil {
		logrus.Error(err)
	}
	return string(result)
}

func JsonString2Struct(s string, v any) {
	err := json.Unmarshal([]byte(s), v)
	if err != nil {
		logrus.Error(err)
	}
}

func JsonMarshalDisableEscapeHtml(data interface{}) (string, error) {
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	if err := jsonEncoder.Encode(data); err != nil {
		return "", err
	}
	return bf.String(), nil
}
