package errors

type ErrorType string

const (
	//UnknownError all kinds of resource unknown error,like unknownType/unknownDatabase/unknownProperty
	UnknownError ErrorType = "UNKNOWN_ERROR"

	// HttpError corresponds to the HTTP status code
	HttpError ErrorType = "HTTP_ERROR"

	// RpcError representing various RPCs error, such as gRPC/pb/thrift
	RpcError ErrorType = "RPC_ERROR"

	// UserError representing various User Errors, such as input parameters/sql syntax
	UserError ErrorType = "USER_ERROR"

	// InternalError SynapseDB internal Errors, such as crash
	InternalError ErrorType = "INTERNAL_ERROR"

	// InsufficientResource representing resource insufficient errors, such as oom/queue full
	InsufficientResource ErrorType = "INSUFFICIENT_RESOURCES"

	// ExternalError representing various DataSource errors, such as mysql/arango/redis related error
	ExternalError ErrorType = "EXTERNAL_ERROR"
)
