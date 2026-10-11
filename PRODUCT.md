# Product definition

## Mission

Give real-time video applications one dependable, inspectable media graph
across interchangeable cameras and embedded boards. A consumer asks for a
role and a media contract; it does not need to know a VID/PID, ISP behaviour,
V4L2 mode quirk or vendor codec API.

## Product invariants

1. Exactly one process owns each physical camera capture session; a configured
   physical source cannot be bound to two graph owners.
2. Probed or sensor-stack-declared capability is not accepted as runtime truth
   until frames, cadence and codec bootstrap have been observed.
3. Hardware codecs are selected from locally discovered components and runtime
   media proof. An embedding application's hardware profile is never evidence.
   Software fallback is never silent.
4. A compressed native archive is preserved when processing is not requested.
5. Processing is performed on decoded NV12 once per graph stage and fanned out
   by role.
6. Slow live or analysis consumers cannot stall the protected archive.
7. Status describes the actual codec, delivery path, cadence and active tone
   mapper, not only the requested configuration.
8. Module-specific behaviour belongs in a source or accelerator provider,
   never in consumer code or a board-name branch.
9. When broker image processing is enabled, ordinary consumers receive the
   processed pixel contract; native passthrough is never an implicit bypass.
10. Runtime mode exclusions are bounded circuit breakers: they are bound to
    both the source capability catalog and executable graph, automatically
    enter half-open retry, and are cleared by successful runtime admission.
11. Candidate, selected and runtime-attested capability are distinct states;
    the public status never collapses them into one boolean.
12. Source, processed and analysis image evidence are separate contracts;
    camera feedback never observes broker-processed pixels.
13. Automatic camera control starts from device-declared defaults and operates
    only inside a range-normalized residual envelope around its baseline, so a
    bounded darkening or brightening correction can always unwind.
14. Stream roles do not imply codecs: schema-v3 defaults `live` and `archive`
    to H265, while H264 exists only when an explicitly named stream requests
    interoperability.
15. Every named stream has one explicit `source_ref`; there is no implicit
    source, global role switch or compatibility reader for earlier schemas.
16. Capture admission, epochs, failure recovery and mode health are isolated by
    source, while scarce discovered hardware is arbitrated across source graphs.
17. Each source is an independent failure domain. Startup or runtime failure
    releases only that source's endpoints and hardware leases; bounded local
    restart never cancels streams bound to another source.
18. Runtime routing accepts only opaque choices from a validated catalog and
    commits them only after source identity, capability, first-frame and media
    readiness proof; failure preserves or restores the previously published
    graph.
19. Application labels, users, recording policy, AI task semantics and device
    inventory resolution stay outside the product. Runtime routing is expressed
    only as generic named sources, named streams, profiles and graph deltas.

## Why it is different

General media servers are strong at network protocols and packaging. Generic
pipeline tools are strong at composing elements. `video-broker` focuses on the
embedded boundary between unreliable camera capability claims, scarce codec
engines, image conditioning, real-time AI frames and interactive streaming.

Its reusable value is the policy layer around that boundary:

- source admission that fingerprints V4L2 discovery or an opaque sensor-stack
  selector, with runtime proof for both and mode quarantine only where a real
  discoverable mode catalog exists;
- role-aware graph selection and protected archive priority;
- source/native versus processed archive semantics;
- spatially adaptive, temporally bounded tone mapping independent of the
  camera ISP;
- codec-generation and bootstrap handling suitable for long-lived consumers;
- portable consumer SDKs and explicit platform adapter contracts.

## Non-goals

The module does not own WebRTC signalling, user identity, recording retention,
MP4 policy, object detection, PTZ decisions or an application service bus. It
publishes video and measured state so those products can evolve independently.

## Standalone readiness

The in-repository module already has its own Go module, commands, native
workers, configuration, protocols, examples and tests. A future repository
split should be mechanical: move `src/video-broker`, choose release packaging,
and keep application adapters outside it. No application-specific import,
machine inventory, installer profile or runtime artifact is allowed as an input
to product discovery.
