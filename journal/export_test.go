package journal

// Classify is the file system check, for a test that has no tmpfs to hand.
var Classify = classify

// MarkEvery sets how many sync rounds pass between two marks, for journals
// opened after the call, and returns a function that restores it. The tests
// write a mark every round so that every crash state has marks in it.
func MarkEvery(n int) func() {
	old := markRounds
	markRounds = n
	return func() { markRounds = old }
}
