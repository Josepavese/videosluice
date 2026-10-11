# Qualification and limits

Linux amd64 and arm64 CI and artifact installer checks cover standalone operation.
The arm64 RDK X5 variant requires the supported vendor SDK and runtime libraries.
An actual 0.1.0 → 0.1.1 installer upgrade preserved configuration and useful data,
retained the previous release and activated the new launcher.

The 0.1.1 integration workload passed 1260.12 seconds with 2521 authenticated
samples, 19370 additional analyzed frames, unchanged source epoch, two archive
finalizations, zero archive/raw drops and no collector overruns. Maximum board
temperature was 77.633 °C. Native telemetry observed accelerated resize with zero
CPU resize frames in sampled intervals. This is one board/profile/workload.

Earlier windows failed on stale audio during concurrent installation and a
system service query waiting for a storage-backed page. The later pass used the
same software, policy and thresholds after I/O settled. It does not certify
uninterrupted platform latency during administrative installation. Complete
application-specific reports remain with their application owners.

The plain framed analysis socket does not qualify as a native causal publication
endpoint: connection alone does not prove delivered frames. Use the authenticated
frame SDK for causal frame and ROI access. Acquisition, negotiated processing and
consumer cadence must be measured on the intended hardware; no universal FPS,
zero-copy or platform-certification claim follows from a fixture or CI result.

Application-specific operational history and integration fixtures belong to their
owners. Product tests exercise the public contracts without their source trees.
