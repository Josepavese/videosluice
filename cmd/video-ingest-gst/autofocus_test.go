package main

import "testing"

func TestLibcameraFocusUsesOnlyCurrentGenericProperty(t *testing.T) {
	if !hasLibcameraAutofocus([]byte("  af-mode             : Autofocus mode\n")) {
		t.Fatal("current property missing")
	}
	for _, text := range []string{"  auto-focus-mode : old property", "random af-mode : text"} {
		if hasLibcameraAutofocus([]byte(text)) {
			t.Fatalf("accepted unavailable property %q", text)
		}
	}
}
