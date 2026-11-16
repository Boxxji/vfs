//go:build !unix

package file

import "errors"

// lock fails closed: writes need an exclusive lock this platform does not provide.
func (f *fileBackend) lock() (func(), error) {
	return nil, errors.New("file backend: store lock needs a unix platform")
}
