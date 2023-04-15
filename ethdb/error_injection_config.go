package ethdb

type ErrorType int

const (
	ErrorRead            ErrorType = iota
	CorruptedReadAllZero           // Read zeroes from the database
	CorruptedReadGarbage           // Read garbage from the database
)

type ErrorInjectionConfig struct {
	EnableInjection bool      `toml:",omitempty"` // Whether to enable error injection
	ErrorType       ErrorType `toml:",omitempty"` // The type of error to inject
	InjectCount     uint      `toml:"-"`          // Which access to inject the error
	FailWhen        uint      `toml:",omitempty"` // The number of accesses to inject the error
}
