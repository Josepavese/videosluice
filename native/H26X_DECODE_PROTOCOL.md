# NHD2 video-broker decode protocol

`NHD2` is the private pipe protocol between an encoded-video consumer and
`rdkx5_h26x_decode_bridge`. It does not replace or version-bump the public
`NMB2` broker contract.

Every record starts with this 32-byte network-byte-order header:

```text
magic[4]="NHD2" | version:u8=2 | kind:u8 | flags:u16 |
frame_id:u32 | timestamp_ns:u64 | frame_gap:u32 |
missed_frames:u32 | payload_size:u32
```

Kinds are `READY=1`, `ENCODED_AU=2`, `DECODED_NV12=3`, `DROPPED=4` and
`CORRELATION_LOST=5`. `READY`, `DROPPED` and a notification-only
`CORRELATION_LOST` have no payload. A decoded payload is exactly
`width * height * 3 / 2` bytes of NV12.

For NHD2 the bridge assigns each input record an internal non-zero PTS and
stores its source metadata in a bounded correlation table. The RDK X5 media
decoder returns that PTS with the decoded frame, including reordered frames,
so the bridge can emit the original broker ID and timestamp rather than infer
them from FIFO position. A rejected input/output emits `DROPPED`; a missing or
unknown returned PTS emits `CORRELATION_LOST` and is never labelled with a
different access-unit ID.

`NHD2` is the only supported framing and is also the bridge default. The
consumer starts the bridge with `--pipe-protocol nhd2` and waits for
`READY` before sending an access unit. A missing or invalid handshake is a
hard protocol failure: the consumer closes the process and retries without
changing framing.

The exact-correlation path depends on the RDK X5 multimedia API preserving
`media_codec_buffer_t.vstream_buf.pts` as decoded
`media_codec_buffer_t.vframe_buf.pts`. The API declares PTS supported for both
decode stream input and frame output. If a runtime violates that contract,
the bridge emits `CORRELATION_LOST` while still forwarding the NV12 frame.
