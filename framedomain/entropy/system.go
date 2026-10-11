// Package entropy is the explicit portable system-CSPRNG provider.
package entropy

import "crypto/rand"

type System struct{}

func (System) Fill(dst []byte) error { _, err := rand.Read(dst); return err }
