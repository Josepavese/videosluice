package ingress_test

import (
	"crypto/sha256"
	fa "github.com/Josepavese/videosluice/frameaccess"
)

func portableIntegrity() fa.PayloadIntegrity { return fa.PayloadIntegrity{Sum256: sha256.Sum256} }
