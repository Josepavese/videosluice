# Encoder SDK teardown

The RDK X5 MediaCodec contract requires `hb_mm_mc_stop` before
`hb_mm_mc_release`. `pause` suspends codec threads and enters PAUSED; it does
not establish the stopped INITIALIZED state required for final release.

The persistent encoder HAL now stops every successfully initialized context,
including cleanup after failed configure/start, before releasing it. Stop or
release failure retains ownership, quarantines the process, and prevents a
replacement allocation. A failed buffer generation still accepts no further
frames or controls. No media timeout, cadence, or recovery bound changes.

The native fixture rejects pause as teardown, requires stop before release,
and covers normal close, repeated close, partial startup, all four queue
failures, stop/release failures, and reuse after confirmed cleanup for H.264
and H.265. The standalone check runs it normally and with ASAN/UBSAN.
Actual hardware teardown and subsequent encoding require separate RDK proof.

This corrects an SDK lifecycle violation. It does not establish the cause of
an output-buffer timeout that occurred before teardown, or of a downstream
interpreter delay. Retrying a poisoned queue or extending its deadline was
rejected because output ownership would remain uncertain.

Primary contract: [X5 codec API, stop/pause/release](https://developer.d-robotics.cc/x5_sdk_doc_v2.0.0/en/multimedia_development/10-MediaCodec_API_zh_CN.html),
sections 5.12.4.8, 5.12.4.9, and 5.12.4.11. The installed SDK header independently
defines STOP as reset to INITIALIZED and PAUSE as transition to PAUSED.
