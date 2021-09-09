package fs

type Status int

const (
	Ok             Status = 0
	ErrPerm               = 1
	ErrIo                 = 5
	ErrNxIo               = 6
	ErrAccess             = 13
	ErrExist              = 17
	ErrNoDev              = 19
	ErrNotDir             = 20
	ErrIsDir              = 21
	ErrFBig               = 27
	ErrNoSpc              = 28
	ErrRoFs               = 30
	ErrNameTooLong        = 63
	ErrNotEmpty           = 66
	ErrDQuot              = 69
	ErrStale              = 70
	ErrWFlush             = 99
)
