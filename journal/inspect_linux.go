package journal

import "syscall"

// inspect reads the type of the file system dir is on and classifies it.
func inspect(dir string) ([]string, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return nil, err
	}
	return classify(uint32(st.Type), dir)
}
