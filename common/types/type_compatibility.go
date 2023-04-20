package types

import (
	"github.com/KineDB/kinedb-client-go/common/errors"
)

type TypeCompatibility struct {
	commonSupperType *StandardType
	coercible        bool
}

func (c *TypeCompatibility) IsCompatible() bool {
	return c.commonSupperType != nil
}

func (c *TypeCompatibility) GetCommonSupperType() StandardType {
	if c.commonSupperType == nil {
		panic(errors.New(errors.TypeIncompatible, "Types are not compatible"))
	}
	return *c.commonSupperType
}

func (c *TypeCompatibility) IsCoercible() bool {
	return c.coercible
}

func Compatible(commonSupperType StandardType, coercible bool) TypeCompatibility {
	return TypeCompatibility{
		commonSupperType: &commonSupperType,
		coercible:        coercible,
	}
}

func Incompatible() TypeCompatibility {
	return TypeCompatibility{
		coercible: false,
	}
}
