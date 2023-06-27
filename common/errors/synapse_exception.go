package errors

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Error = SynapseException

// SynapseException Wrap errorCode/message/stack
type SynapseException interface {
	// Error implement error interface
	Error() string

	ErrorCode() StandardErrorCode

	Message() string

	StackTrace() errors.StackTrace

	WithCode(errCode StandardErrorCode) SynapseException

	WithCause(err error) SynapseException
}

type synapseException struct {
	errorCode StandardErrorCode
	message   string
	stack     errors.StackTrace
}

func (e *synapseException) ErrorCode() StandardErrorCode {
	return e.errorCode
}

func (e *synapseException) Message() string {
	return e.message
}

func (e *synapseException) StackTrace() errors.StackTrace {
	return e.stack
}

func (e *synapseException) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		// Print value format:  <type> <name>:<code> <message> <stackTrace>
		fmt.Fprintf(s, "%s %s:%d %+v %+v",
			string(e.errorCode.Type()),
			e.errorCode.Name(),
			e.errorCode.Code(),
			e.Message(),
			restrictStackTrace(e.StackTrace()))
	case 's':
		io.WriteString(s, e.message)
	case 'q':
		fmt.Fprintf(s, "%q", e.message)
	}
}

func (e *synapseException) Error() string {
	return fmt.Sprintf("%s %s:%d \n%+v %+v",
		string(e.errorCode.Type()),
		e.errorCode.Name(),
		e.errorCode.Code(),
		e.Message(),
		restrictStackTrace(e.StackTrace()),
	)
}

func (e *synapseException) WithCode(errCode StandardErrorCode) SynapseException {
	e.errorCode = errCode
	return e
}

func (e *synapseException) WithCause(err error) SynapseException {
	e.message = e.message + ". Cause: " + err.Error()
	if s, ok := err.(stackTracer); ok {
		e.stack = s.StackTrace()
	}
	return e
}

func restrictStackTrace(stack errors.StackTrace) errors.StackTrace {
	if len(stack) > maxErrorStackTraceSize {
		stack = stack[0:maxErrorStackTraceSize]
	}
	return stack
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func New(errorCode StandardErrorCode, message string) SynapseException {
	return &synapseException{
		errorCode: errorCode,
		message:   message,
		stack:     errors.New("").(stackTracer).StackTrace()[1:],
	}
}

func Newf(errorCode StandardErrorCode, format string, args ...interface{}) SynapseException {
	if len(args) == 0 {
		return New(errorCode, format)
	}
	message := fmt.Sprintf(format, args...)
	return New(errorCode, message)
}

func GetErrorCode(code uint32) uint32 {
	return code / 1000
}

func GetHttpCode(code uint32) uint16 {
	return uint16(code % 1000)
}

func ConvertSynapseExceptionToGrpcError(synapseException error) error {
	if synapseException == nil {
		return nil
	}

	var (
		errCode StandardErrorCode
		message string
	)
	e, ok := status.FromError(synapseException)
	if !ok || e.Err() == nil {
		return New(GenericUnknownError, synapseException.Error())
	}
	msg := e.Message()

	switch e.Code() {
	case codes.Unavailable:
		return New(GenericRpcError, msg)
	}

	msgForCodeSlice := strings.Split(msg, " ")
	if len(msgForCodeSlice) > 2 {
		errType := ErrorType(msgForCodeSlice[0])
		nameAndCode := strings.Split(msgForCodeSlice[1], ":")
		code, _ := strconv.Atoi(nameAndCode[1])
		errCode = NewStandardErrorCode(ErrorType(errType), nameAndCode[0], uint32(code))
		if code == 0 {
			errCode = GenericRpcError
		}
	}
	msgSlice := strings.Split(msg, "\n")
	if len(msgSlice) > 2 {
		message = strings.TrimSpace(msgSlice[1])
	}

	err := New(errCode, message)
	return err
}

func Recover(err any) Error {
	//log.CollectLog(cache.ERROR, err)
	switch err.(type) {
	case Error:
		return err.(SynapseException)
	default:
		return Newf(GenericInternalError, "panic: %+v", err)
	}
}
