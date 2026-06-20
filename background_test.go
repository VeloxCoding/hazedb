package hazedb

import "testing"

// runRecovered must contain a panic raised inside a background goroutine so the
// host process survives (an escaped panic would crash this test binary), and must
// run a non-panicking fn unchanged.
func TestRunRecoveredContainsPanic(t *testing.T) {
	// If the panic escaped runRecovered, the goroutine would abort the process and
	// `done` would never be sent; reaching the receive proves it was recovered.
	done := make(chan bool, 1)
	go func() {
		runRecovered("test", func() { panic("boom") })
		done <- true
	}()
	if !<-done {
		t.Fatal("unreachable")
	}

	// A normal fn runs to completion.
	ran := false
	runRecovered("test", func() { ran = true })
	if !ran {
		t.Fatal("runRecovered did not run fn")
	}
}
