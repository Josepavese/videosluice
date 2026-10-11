package config

import "testing"

func frameAccessConfig(t *testing.T) Config {
	t.Helper()
	cfg := validConfig()
	stream := cfg.Streams["analysis"]
	delete(cfg.Streams, "analysis")
	cfg.Streams["vision-detail"] = stream
	cfg.Runtime.FrameAccess = &FrameAccessConfig{CopyProvider: "cpu_nv12_copy", Readers: []FrameReaderConfig{
		{ID: "consumer-a", Stream: "vision-detail", DeliverySocket: "/run/video-broker/consumer.frames", ROISocket: "/run/video-broker/consumer.roi"},
	}}
	cfg, err := Normalize(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestFrameAccessGenericReadersAndDetachedConfig(t *testing.T) {
	original := frameAccessConfig(t)
	for _, mode := range []string{"clone", "normalize", "runtime"} {
		t.Run(mode, func(t *testing.T) {
			var result Config
			var err error
			switch mode {
			case "clone":
				result = Clone(original)
			case "normalize":
				result, err = Normalize(original)
			case "runtime":
				result.Runtime, err = NormalizeRuntime(original.Runtime)
			}
			if err != nil {
				t.Fatal(err)
			}
			result.Runtime.FrameAccess.CopyProvider = "mutated"
			result.Runtime.FrameAccess.Readers[0].Stream = "mutated"
			if original.Runtime.FrameAccess.CopyProvider != "cpu_nv12_copy" || original.Runtime.FrameAccess.Readers[0].Stream != "vision-detail" {
				t.Fatal("mutable frame access escaped clone")
			}
		})
	}
}

func TestFrameAccessRejectsInvalidAdmissionAndEndpointCollisions(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"implicit-copy":  func(c *Config) { c.Runtime.FrameAccess.CopyProvider = "auto" },
		"missing-reader": func(c *Config) { c.Runtime.FrameAccess.Readers = nil },
		"unknown-stream": func(c *Config) { c.Runtime.FrameAccess.Readers[0].Stream = "missing" },
		"encoded-stream": func(c *Config) { c.Runtime.FrameAccess.Readers[0].Stream = "live" },
		"duplicate-id": func(c *Config) {
			c.Runtime.FrameAccess.Readers = append(c.Runtime.FrameAccess.Readers, c.Runtime.FrameAccess.Readers[0])
		},
		"relative": func(c *Config) { c.Runtime.FrameAccess.Readers[0].ROISocket = "roi.sock" },
		"same-endpoint": func(c *Config) {
			c.Runtime.FrameAccess.Readers[0].ROISocket = c.Runtime.FrameAccess.Readers[0].DeliverySocket
		},
		"stream-endpoint": func(c *Config) { c.Runtime.FrameAccess.Readers[0].ROISocket = c.Streams["live"].Socket },
		"status-endpoint": func(c *Config) { c.Runtime.Status.Socket = c.Runtime.FrameAccess.Readers[0].ROISocket },
		"governor-state":  func(c *Config) { c.Runtime.Governor.StatePath = c.Runtime.FrameAccess.Readers[0].ROISocket },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := frameAccessConfig(t)
			mutate(&cfg)
			if err := Validate(cfg); err == nil {
				t.Fatal("invalid frame configuration admitted")
			}
		})
	}
}
