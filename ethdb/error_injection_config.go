package ethdb

type ErrorType int

const (
	ErrorRead            ErrorType = iota
	CorruptedReadAllZero           // Read zeroes from the database
	CorruptedReadGarbage           // Read garbage from the database
)

type ErrorInjectionConfig struct {
	EnableInjection bool      // Whether to enable error injection
	ErrorType       ErrorType // The type of error to inject
	InjectCount     uint      // Which access to inject the error
	FailWhen        uint      // The number of accesses to inject the error
}
