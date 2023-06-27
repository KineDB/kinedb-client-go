package types

import "github.com/KineDB/kinedb-client-go/common/errors"

/*
	SynapseDB Build-in unified standard types name for SQL
*/
// TODO make all standard type a byte code only？
const (
	EMPTY                    StandardType = "empty"
	UNKNOWN                  StandardType = "unknown"
	BIGINT                   StandardType = "bigint"
	INTEGER                  StandardType = "integer"
	SMALLINT                 StandardType = "smallint"
	TINYINT                  StandardType = "tinyint"
	BOOLEAN                  StandardType = "boolean"
	DATE                     StandardType = "date"
	DECIMAL                  StandardType = "decimal"
	REAL                     StandardType = "real"
	DOUBLE                   StandardType = "double"
	HYPER_LOG_LOG            StandardType = "HyperLogLog"
	QDIGEST                  StandardType = "qdigest"
	TDIGEST                  StandardType = "tdigest"
	P4_HYPER_LOG_LOG         StandardType = "P4HyperLogLog"
	INTERVAL_DAY_TO_SECOND   StandardType = "interval day to second"
	INTERVAL_YEAR_TO_MONTH   StandardType = "interval year to month"
	TIMESTAMP                StandardType = "timestamp"
	TIMESTAMP_MICROSECONDS   StandardType = "timestamp microseconds"
	TIMESTAMP_WITH_TIME_ZONE StandardType = "timestamp with time zone"
	TIME                     StandardType = "time"
	TIME_WITH_TIME_ZONE      StandardType = "time with time zone"
	VARBINARY                StandardType = "varbinary"
	VARCHAR                  StandardType = "varchar"
	CHAR                     StandardType = "char"
	ROW                      StandardType = "row"
	ARRAY                    StandardType = "array"
	MAP                      StandardType = "map"
	JSON                     StandardType = "json"
	IPADDRESS                StandardType = "ipaddress"
	IPPREFIX                 StandardType = "ipprefix"
	GEOMETRY                 StandardType = "Geometry"
	BING_TILE                StandardType = "BingTile"
	BIGINT_ENUM              StandardType = "BigintEnum"
	VARCHAR_ENUM             StandardType = "VarcharEnum"
	DISTINCT_TYPE            StandardType = "DistinctType"
	UUID                     StandardType = "uuid"
	DATETIME                 StandardType = "datetime"
)

/*
	for aggregation function partial result
*/
const (
	AGGREGATION_STATE_TYPE StandardType = "AggregationStateType"
)

func StandardTypeToCode(t StandardType) byte {
	switch t {
	case EMPTY:
		return 0
	case UNKNOWN:
		return 1
	case BIGINT:
		return 2
	case INTEGER:
		return 3
	case SMALLINT:
		return 4
	case TINYINT:
		return 5
	case BOOLEAN:
		return 6
	case DATE:
		return 7
	case DECIMAL:
		return 8
	case REAL:
		return 9
	case DOUBLE:
		return 10
	case HYPER_LOG_LOG:
		return 11
	case QDIGEST:
		return 12
	case TDIGEST:
		return 13
	case P4_HYPER_LOG_LOG:
		return 14
	case INTERVAL_DAY_TO_SECOND:
		return 15
	case INTERVAL_YEAR_TO_MONTH:
		return 16
	case TIMESTAMP:
		return 17
	case TIMESTAMP_MICROSECONDS:
		return 18
	case TIMESTAMP_WITH_TIME_ZONE:
		return 19
	case TIME:
		return 20
	case TIME_WITH_TIME_ZONE:
		return 21
	case VARBINARY:
		return 22
	case VARCHAR:
		return 23
	case CHAR:
		return 24
	case ROW:
		return 25
	case ARRAY:
		return 26
	case MAP:
		return 27
	case JSON:
		return 28
	case IPADDRESS:
		return 29
	case IPPREFIX:
		return 30
	case GEOMETRY:
		return 31
	case BING_TILE:
		return 32
	case BIGINT_ENUM:
		return 33
	case VARCHAR_ENUM:
		return 34
	case DISTINCT_TYPE:
		return 35
	case UUID:
		return 36
	case DATETIME:
		return 37
	case AGGREGATION_STATE_TYPE:
		return 99
	default:
		panic(errors.Newf(errors.UnknownType, "StandardTypeToCode typeStr unKnown %s", string(t)))
	}
	return 1
}

func StandardTypeFromCode(code byte) StandardType {
	switch code {
	case 0:
		return EMPTY
	case 1:
		return UNKNOWN
	case 2:
		return BIGINT
	case 3:
		return INTEGER
	case 4:
		return SMALLINT
	case 5:
		return TINYINT
	case 6:
		return BOOLEAN
	case 7:
		return DATE
	case 8:
		return DECIMAL
	case 9:
		return REAL
	case 10:
		return DOUBLE
	case 11:
		return HYPER_LOG_LOG
	case 12:
		return QDIGEST
	case 13:
		return TDIGEST
	case 14:
		return P4_HYPER_LOG_LOG
	case 15:
		return INTERVAL_DAY_TO_SECOND
	case 16:
		return INTERVAL_YEAR_TO_MONTH
	case 17:
		return TIMESTAMP
	case 18:
		return TIMESTAMP_MICROSECONDS
	case 19:
		return TIMESTAMP_WITH_TIME_ZONE
	case 20:
		return TIME
	case 21:
		return TIME_WITH_TIME_ZONE
	case 22:
		return VARBINARY
	case 23:
		return VARCHAR
	case 24:
		return CHAR
	case 25:
		return ROW
	case 26:
		return ARRAY
	case 27:
		return MAP
	case 28:
		return JSON
	case 29:
		return IPADDRESS
	case 30:
		return IPPREFIX
	case 31:
		return GEOMETRY
	case 32:
		return BING_TILE
	case 33:
		return BIGINT_ENUM
	case 34:
		return VARCHAR_ENUM
	case 35:
		return DISTINCT_TYPE
	case 36:
		return UUID
	case 37:
		return DATETIME
	case 99:
		return AGGREGATION_STATE_TYPE
	default:
		panic(errors.Newf(errors.UnknownType, "StandardType Code  unKnown %d", code))
	}
	return UNKNOWN
}
