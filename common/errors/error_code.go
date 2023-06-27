package errors

const (
	maxErrorStackTraceSize = 10
)

// StandardErrorCode Expected errorCode log format "<error_type> <error_name>:<error_code>"
type StandardErrorCode interface {
	Type() ErrorType
	Code() uint32
	Name() string
}

// standardErrorCode Expected error log like "<error_type> <error_name>:<error_code>"
type standardErrorCode struct {
	errorType ErrorType
	name      string
	code      uint32
}

func (c *standardErrorCode) Type() ErrorType {
	return c.errorType
}

func (c *standardErrorCode) Name() string {
	return c.name
}

func (c *standardErrorCode) Code() uint32 {
	return c.code
}

func NewStandardErrorCode(errorType ErrorType, name string, code uint32) StandardErrorCode {
	return &standardErrorCode{
		errorType: errorType,
		name:      name,
		code:      code,
	}
}
