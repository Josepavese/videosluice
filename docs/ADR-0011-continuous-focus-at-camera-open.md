# Continuous autofocus at camera open

Camera firmware may power up with autofocus disabled even when the lens supports
it. Leaving all camera controls untouched therefore does not guarantee usable
focus after attachment or a source-local restart.

The source schema defaults `autofocus` to `continuous`; explicit `camera` leaves
an operator's calibration unchanged. Media adapters own the one-shot request at
camera acquisition. Native V4L2 queries the focus capability and reads the mode
on the capture descriptor, writes only if disabled and verifies it. Portable
V4L2 targets the same stable control ID. Libcamera uses the current generic
AfMode property, whose adapter filters unsupported camera controls. Other sensor
stacks cannot claim support without an actuator/algorithm control API.

This is independent of ISP auto-exposure, white balance and image enhancement.
Broad UVC discovery, periodic firmware reconciliation, model/VID/PID lists and
application startup scripts were rejected: they add unrelated control traffic
or miss source-local reopen. Absent focus is optional; failed initialization is
diagnostic, not a fabricated success or a reason to withhold video.

There is no persistent-data or frame-protocol change. Qualification includes
source-default/override validation, worker propagation, absent/disabled/already
enabled/readback failure tests, native ioctl contract with sanitizers and actual
camera startup. Firmware mode acceptance and a physically focused image remain
distinct observations. Libcamera without the generic property is explicitly
unavailable and requires a backend upgrade before it can provide this feature.
