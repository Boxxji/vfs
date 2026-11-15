package journal

import "fmt"

// File system magic numbers (statfs(2)).
const (
	tmpfsMagic   = 0x01021994
	ramfsMagic   = 0x858458f6
	overlayMagic = 0x794c7630
	fuseMagic    = 0x65735546
	nfsMagic     = 0x6969
	smbMagic     = 0x517b
	cifsMagic    = 0xff534d42
	p9Magic      = 0x01021997
)

// classify refuses a directory on memory and names the risks of a layered or
// remote one, where a sync may not reach the media. magic is the type statfs(2)
// reports for the file system the directory is on.
func classify(magic uint32, dir string) ([]string, error) {
	switch magic {
	case tmpfsMagic, ramfsMagic:
		return nil, fmt.Errorf("%w: %s", ErrVolatile, dir)
	case overlayMagic:
		return []string{dir + " is on an overlay file system; a sync reaches the media only if the layer below passes it on"}, nil
	case fuseMagic, p9Magic:
		return []string{dir + " is on a user-space or guest-shared file system (FUSE, 9p, virtio-fs); a sync reaches the media only if the host does what the guest asks"}, nil
	case nfsMagic, smbMagic, cifsMagic:
		return []string{dir + " is on a network file system; a sync reaches the media only if the server does what the client asks"}, nil
	}
	return nil, nil
}
