# FAP copy-budget expiry retires a fully consumed source channel

Status: **OPEN — corrected; local integration/race and test board suites passed; integrated runtime qualification pending**.

## Real failure

Signed test board candidate f97b5478d5e5c46ea6547526a9acbb5bd27db204f273ed45efe866ef066921ca,
sequence1789020113081899044, failed the integrated trial10September07:00UTC.
Python, compile, camera discovery and Event-AI BPU/ONNX passed. Postflight
rejected `task 6 reported exits=1`; updater controller2798 and its unit later
exited1 and automatically recovered clean9ffc2888. This is not a passed release.

Source overview, epoch1, origin2076: all192records/3110400payload bytes were
received and integrity-checked. Receiver430438us against999977us budget;
domain begin384140us, store61us, rejection at surface admission, with neither
context cancellation nor context deadline. The separately admitted copy budget
is100ms, receive budget1000ms. Copy-budget expiry propagated as fatal receive
failure, retired capture and required Tracker rebind/process restart.

Native analysis subsequently reported send io_deadline; that is downstream of
the receiver failure, not evidence that the initial full frame was incomplete.
Earlier Tracker summaries were17.33/17.95/18.47fps, recognition_errors0 and
human inference medians27.86/28.20/28.15ms; these are not proof of sustained20
completed inferences/sec. Last summary max80.3C, not a thermal-stop trigger.
No people were detected in those summaries; they do not qualify fresh Registry ROI.

## Correction and boundaries

- Preserve the channel only for a fully consumed, verified surface rejected at
  `surface_store` for the local copy budget, with current authenticated binding
  and a still-live receive context before AND after the final binding check.
- Count a failed/dropped surface, never a successfully retained one. No replay,
  retry, queue, fabricated surface, source substitution or software fallback.
  The missing exact ROI remains expired/unavailable; the next frame can proceed.
- Tighten existing capacity/retention drop classification to actual surface
  store or output-binding failures. Clock regression, partial/invalid packets,
  transport/consumer deadlines, cancellation and retirement stay fatal.
- Log verified-frame drops at counts1,2,4,8,... with source/epoch, cause and
  continuation action; no healthy-frame logging flood.

The384ms domain-begin delay itself remains to explain and measure. This patch
does not certify the SD, eliminate all contention or close thermal/performance
N6. It does not change100ms/1000ms budgets, thermal thresholds, health gates,
profiles, corpus or accelerator policy, and does not excuse a Tracker exit.

## Evidence and remaining acceptance

Regression first RED: the next Receive failed `source_receive_closed` after a
copy deadline. Added deterministic pre-admission budget-expiry coverage, next
exact-frame/ROI proof, and cancellation/clock-regression fatal cases.
Integration Go suites frameingress/framedomain/frameaccess/cmd/internal-broker
passed after correction. Final race50604 EXIT0 on the same five packages;
actual onboard continuation still required. Source hash manifest verified.
10September onboard: broker and native bridges compiled on test board; the same five
Go suites passed there (7.869s/0.091s/0.036s/43.320s/35.105s respectively).
The last suite was resumed without rebuilding after the runner's aggregate
180s budget expired; individual package timeouts remained90s. No thermal stop
or service restart occurred during compilation/testing (maximum76.559C).
Go binary a4b517bd98550d91dc87bc9a47013289f0bdab53e8493036164d8e6066ee781a;
payload manifest bd75a6b34ec39861aff993bb79a8c93c40c14d1aac3c390024be40f9c3c9b303.
Development promotion started07:40UTC using the existing transactional tool;
at07:46 it was still copying/fsyncing the measured release, not yet applied.
The older baseline's drain rejected transient D immediately; the already-tested
signed drain correction completed a no-active-leaf stop within unchanged bounds.
This does not prove the copy-budget drop's actual runtime continuation or
qualify the complete signed candidate; issue remains open.

Promotion subsequently completed, exact measurement9285entries/552880319bytes
passed and the service restarted on the intended development root. Integrated
diagnostic61samples/300.210546s FAILED: a different, partial source transfer
timed out and Tracker restarted at temperature below72C. No verified-frame
copy-budget drop was exercised in that failing epoch (drops0).
See [native transfer stall](2026-09-10-native-surface-transfer-stalls-analysis.md).
The correction remains valid for its tested case but is not sufficient for
integrated stability; do not declare this issue or the goal complete.

Evidence outside Git:
`<private-workspace>`:
`test-board-f97b54-trial-{journal.txt,failure-root-log.txt}`,
`test-board-f97b54-observation-13.json`, `test-board-f97b54-recovery-progress-1.log`,
`surface-copy-budget-retirement-{red,integration-final,race}.log`.
