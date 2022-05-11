package importer

// RetryOptions represents the retry behavior of individual dynamodb:BatchWriteItem actions.
type RetryOptions struct {
	// BackOffBase a component of back-off duration time.
	// The duration is calculated according to the following formula:
	//
	//			duration = random_between(0, base * 2 ** attempt)
	//
	BackOffBase int

	// MaxAttempt and Timeout(ms) determine the end of retry.
	// The minimum one of them is adopted.
	MaxAttempt int
	Timeout    int
}

var defaultRetryOptions = RetryOptions{
	BackOffBase: 100,
	MaxAttempt:  8,
	Timeout:     60_000,
}

// WithBackOffBase provides the way to update RetryOptions.BackOffBase
func WithBackOffBase(base int) func(*RetryOptions) {
	return func(opt *RetryOptions) {
		if base != 0 {
			opt.BackOffBase = base
		}
	}
}

// WithMaxAttempt provides the way to update RetryOptions.MaxAttempt
func WithMaxAttempt(attempt int) func(*RetryOptions) {
	return func(opt *RetryOptions) {
		if attempt != 0 {
			opt.MaxAttempt = attempt
		}
	}
}

// WithTimeout provides the way to update RetryOptions.Timeout
func WithTimeout(timeout int) func(*RetryOptions) {
	return func(opt *RetryOptions) {
		if timeout != 0 {
			opt.Timeout = timeout
		}
	}
}
