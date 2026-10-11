# Third-party dependencies

Go modules are pinned by `go.mod` and `go.sum`: Pion RTP/randutil (MIT) and
`golang.org/x/sys` (BSD-3-Clause). Distribution includes their license notices
under `licenses/`, copied from the exact resolved module cache by the builder.

Native runtime links to system GStreamer (LGPL), OpenSSL (Apache-2.0 on version
3), optional OpenCL loader and, on RDK X5, the vendor media SDK and TurboJPEG.
Those system libraries, drivers and SDKs are not redistributed in release
assets. Install their supported distribution packages and observe their terms.
