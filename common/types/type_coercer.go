package types

func GetCommonSuperType(firstType StandardType, secondType StandardType) StandardType {
	typeCompatibility := compatibility(firstType, secondType)
	if !typeCompatibility.IsCompatible() {
		return UNKNOWN
	}
	return typeCompatibility.GetCommonSupperType()
}

func CanCoerce(fromType StandardType, toType StandardType) bool {
	typeCompatibility := compatibility(fromType, toType)
	return typeCompatibility.IsCoercible()
}

func CoerceTypeBase(sourceType StandardType, resultType StandardType) StandardType {
	if sourceType == resultType {
		return sourceType
	}
	switch sourceType {
	case UNKNOWN:
		// UNKNOWN incompatible with every type
		return UNKNOWN
	case EMPTY:
		// EMPTY means NULL, it is compatible with every type
		return resultType
	case TINYINT:
		switch resultType {
		case SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DECIMAL:
			return resultType
		}
	case SMALLINT:
		switch resultType {
		case INTEGER, BIGINT, REAL, DOUBLE, DECIMAL:
			return resultType
		}
	case INTEGER:
		switch resultType {
		case BIGINT, REAL, DOUBLE, DECIMAL:
			return resultType
		}
	case BIGINT:
		switch resultType {
		case REAL, DOUBLE, DECIMAL:
			return resultType
		}
	case DECIMAL:
		switch resultType {
		case REAL, DOUBLE:
			return resultType
		}
	case REAL:
		switch resultType {
		case DOUBLE:
			return resultType
		}
	case VARCHAR:
		switch resultType {
		case CHAR, TIMESTAMP, TIME, DATE, JSON, DATETIME:
			return resultType
		}
	case CHAR:
		switch resultType {
		case VARCHAR, JSON:
			return resultType
		}
	case DATE:
		switch resultType {
		case TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE:
			return resultType
		}
	case TIME:
		switch resultType {
		case TIMESTAMP_WITH_TIME_ZONE:
			return resultType
		}
	case TIMESTAMP:
		switch resultType {
		case TIMESTAMP_WITH_TIME_ZONE:
			return resultType
		}
	case DATETIME:
		switch resultType {
		case TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP:
			return resultType
		}
	default:
		// TODO implement compatibility of more types
		return UNKNOWN
	}
	return UNKNOWN
}

func CanCompatible(fromType StandardType, toType StandardType) bool {
	typeCompatibility := compatibility(fromType, toType)
	return typeCompatibility.IsCompatible()
}

func compatibility(firstType StandardType, secondType StandardType) TypeCompatibility {
	fromType := firstType
	toType := secondType
	if fromType == toType {
		return Compatible(fromType, true)
	}

	// EMPTY means NULL, NULL can compatible with every type
	if fromType == EMPTY {
		return Compatible(toType, true)
	}
	if toType == EMPTY {
		return Compatible(fromType, true)
	}

	// upgrade fromType
	fromTypeCoercedType := CoerceTypeBase(fromType, toType)
	if fromTypeCoercedType != UNKNOWN {
		return compatibility(fromTypeCoercedType, toType)
	}

	// upgrade toType
	toTypeCoercedType := CoerceTypeBase(toType, fromType)
	if toTypeCoercedType != UNKNOWN {
		return compatibility(toTypeCoercedType, fromType)
	}

	return Incompatible()
}
