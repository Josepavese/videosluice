# test board: startup nativo fallito e rilascio non confermato

Stato: **APERTO — recovery e continuità firmata PASS; causa startup e resistenza alla recidiva non dimostrate**.
Collegata: [check del 16 settembre](2026-09-16-test-board-check-bridge-exit64-and-boot-ipc-counter.md).

**Esito soak firmato6ottobre11:13:55CEST: PASS.** Report CLI
`nemesi.recovery-soak-result.v3`, run `0a433cdc9a456f2686db2c81cfd8741c`,
SHA256 `31e26c124713371833247c5352ac0a2848b979f30b867b2b2452dbb424b6b3a8`,
nel PAL `test-board-signed-soak-20261006-r1`. Durata2700,094s,5399campioni,
intervallo0,5s e gate originali; zero overrun/turnover/drop archive/raw,
massimo84,424°C. Tracker52805frame/19,557FPS medi, Recorder40490write
archive e cinque finalizzazioni senza errori. Root firmata/sequence/artifact
e receipt centrale verificati; misura9456entry/554994438byte PASS,
quattro config identiche alla baseline. Readback successivo autenticato:
30,02/19,98/15,01FPS nei tre stream, Tracker18,94FPS nella sola finestra20s,
Registry462ingressi/claim/completamenti e integrità0, tutti starts1/exits0.
Il massimo sync checkpoint7020ms nel soak e7792ms dopo resta diagnostica,
non diagnosi di guasto SD. MP4 chiuso `20261006-090000.mp4` UTC, acquisito
interamente09:00–09:10UTC dopo baseline08:28:54UTC e prima della fine soak:
8998frame HEVC640x360/15FPS e Opus, circa600s. SHA dispositivo/host
`45d27d3fa6fe1e05c60319219e46b21c6f8728609bbbbf03a8f536425f7cc28f`;
ffprobe e decode completo host a un thread PASS/exit0/24s, stderr vuoto,
progress=end/8998frame/dup0/drop0. Nessuna immagine personale visualizzata.
Prima copia rsync respinta per ACL directory: nessun chmod o copia SD;
lettura privilegiata canonica su host completata con lo stesso SHA.
Prove private `test-board-signed-soak-final-readback-20261006-r1.json`,
`test-board-signed-soak-receipt-final-20261006-r1.json`,
`test-board-signed-soak-runtime-final-20261006-r1.json`,
`test-board-signed-soak-final-validation-20261006-r1.json`,
`test-board-signed-soak-mp4-remote-20261006-r1.log`,
`test-board-signed-soak-mp4-host-20261006-r1.json`,
`test-board-signed-soak-decode-host-20261006-r1.result.json/.progress/.stderr`;
stesso pack già indicato. Prova su host distinta dal runtime/acceleratori
test board; il browser VOD resta backlog, non è implicitamente verificato.
Monitor di esito sospeso, **non il goal**. Nessun retry/fault/deploy per
questa verifica; FAILED precedenti conservati. Il PASS dimostra continuità
su questa root dopo power-cycle e recovery: non dimostra la causa della
precedente attesa pre-main né chiude ROI fisiche, pressione SDK o il goal.
La cronologia «in corso» seguente è superata soltanto per questo soak.

**Esito recovery6ottobre:** finalizer/postflight PASS e journal
`healthy`/update `applied`, stessa root/artifact/sequence firmati, quattro
config preservate; transazione originale343e9788… conclusa dal recovery
canonico, non da un retry del deploy. Guard/inhibitor assenti. Supervisor19278
invocation `7e893a46bde24a4696a4f66ccd1a3124`, NRestarts0; Tracker/Recorder/
Registry in avanzamento e26ingressi/completamenti Registry/integrità0.
Due campioni autenticati20s: live30/analysis20/archive15FPS, Tracker17,69 e
17,83FPS; CPU31,97/31,93%, iowait7,04/7,94%, max77,096/77,780°C.
Non inferire target Tracker20FPS stabile o causa SD dalla sola misura breve.

Soak distinto **IN CORSO**, CLI canonico2700s/0,5s, unit
`nemesi-test-board-signed-soak-20261006-r1.service` partita10:28:54CEST,
collector22127/invocation `4a5c445104e34aeaa9e4411fe11f89f7`, PAL privato
`/var/lib/nemesi/state/operator-backups/test-board-signed-soak-20261006-r1/`;
nessun secondo collector, fault o riduzione dei gate. Il recupero operativo
non chiude questa issue o il goal: causa della precedente attesa bootstrap,
resistenza alla recidiva/SDK e ROI reali restano da qualificare.
Prove private `test-board-finalized-readback-20261006-r1.json`,
`test-board-restored-runtime-audit-20261006-r1/r2.json`, `test-board-finalizer-progress-20261006-r1.log`,
`test-board-signed-soak-launch/progress-20261006-r1.*`. Monitor di esito
`qualifica-test-board-firmata` attivo, quello vecchio non esiste più nell'app.

**Ripresa6ottobre:** power-cycle fisico ora confermato dall'utente, nuovo boot
verificato. Controller r1 FAILED prima del move per ownership999 dell'archivio
reset; correttiva del solo tool amministrativo e10test PASS, controller r2
guard recovery PASS10:15:19 sotto lock con root/quattro config preservate.
Incidente e receipt archiviati nel PAL privato, vecchi archivi intatti.
[Finding amministrativo chiuso, non il guasto video](closed/2026-10-06-capture-reset-archive-service-ownership.md).
Recovery canonico PID17294 in corso; nessun nuovo deploy/build, postflight e
progresso capture/Tracker/Recorder ancora da verificare. Goal generale aperto.

## Riavvio comunicato e verifica read-only — 6ottobre2026

L'utente comunica «riavviato»; il readback09:47–09:49CEST verifica nuovo boot
`861712c9-2b31-4945-8b01-bd5610874ce2`, ma non distingue da solo un reboot
software dal reset fisico richiesto. Conferma del metodo chiesta prima di
usare `--confirm-hardware-power-cycle`; nessuna attestazione inventata.

Root firmata precedente conservata `1791219315548914929-57f3e5e100ec6605`,
release `host_committed`, update `preparing` della stessa transazione;
misura canonica indipendente PASS9456entry/554994438byte, quattro hash config
identici. Guard vecchio `993482-1`/SHA `afd16cc44aa4…` ancora presente.
L'inhibitor runtime non è presente dopo il boot; non è stato rimosso da noi
e non sana la quarantena persistente del provider. Nessun deploy/build attivo
nel campione; boot recovery `active/exited` MainPID0, finalizer/trial inattivi.

Snapshot supervisor autenticato e invocation verificata:
PID6937/invocation `ca92cd0820db4606bfadba4e4dc5ddd8`, NRestarts3;
Tracker PID0/starts0/exits0; Recorder PID7058/state recording ma
archive writes0/queue0/drop0; Registry PID6977 running/input0/completed0/
integrity0. Non è video funzionante, qualifica positiva o nuova identificazione.
Massimo termico58,874°C, non benchmark di pipeline video attiva.

Prima lettura r1 interrompe correttamente `systemd_identity` per NRestarts3.
R2 usa una lettura systemd diagnostica e conserva il gate restart FALSE,
autentica socket/PID/UID/invocation e verifica la stessa invocation dopo la
lettura; non modifica il collector o le soglie di postflight/soak.
R1 resta FAILED come sonda, non un soak avviato. Nessuna immagine, mutazione
del corpus, reset, restart o eliminazione del guard eseguiti.

Prove nel pack privato: `test-board-post-reboot-20261006-r1/r2.log`,
`test-board-reboot-readback-20261006-r1.py/.stderr`,
`test-board-reboot-readback-20261006-r2.py/.json`,
`test-board-post-reboot-measure-20261006-r1.json`, recovery access log privato.
Primo SSH rifiutato per troppe chiavi/cache recovery assente; export centrale
canonico concluso e successivo SSH/sudo PASS. Non un guasto di rete/password.
Prossimo passo resta la conferma di power-cycle, poi recovery guard sotto
lock e finalizzazione canonica della root già costruita, non nuovo deploy.

## Ricaduta durante il rilascio firmato — 5ottobre2026

Stato corrente21:15CEST: test board **fermo/non registra**, guard HW e inhibitor
trattenuti; deployed board escluso e intatto. Questo finding resta nel goal N2/N5/N6,
non chiuso o attribuito automaticamente alla SD segnalata a fine vita.

Artefatto r2 firmato SHA `57f3e5e100ec66057ac2e5fc8a0ac3d48f6f7aaab75c149a7284d4cc939bebdf`,
sequence1791219315548914929. Python495s/compile1185s/Event exit0; trial989974
stabile15s/accesso/token PASS. Nel successivo avvio canonico993403/invocation
`8e6ff65729304aef8bf5dbcaf1292378`, native993751 non arriva alla readiness:

- bootstrap avviato21:08:35, budget5000ms originale;
- 21:08:40 osservazione prima della cancellazione: D,1thread,
  TERM non caught/non blocked, `__wait_on_buffer`;
- mailbox `receive_idle_deadline`, stderr vuoto, Wait `signal: terminated`;
- nessuna ricevuta SDK; guard `993482-1` persistito21:08:40.894594659;
- Tracker rimane `source_pending`,40tentativi, escalation21:09:53;
- postflight finale FAIL (`activating pid=0`), updater/controller terminali;
  recovery non prova quiescence al confine e trattiene il candidato.

La diagnostica di startup ora realmente installata distingue l'assenza
dell'handler da una normale attesa di frame. Non prova ancora quale pagina,
binario/DSO, costruttore o chiamata fosse in attesa, né che il loader sia la
causa esatta. La cattura era arrivata alla readiness nella trial, quindi non
è una incompatibilità deterministica dello schema health Registry25campi.
Non aumentare il budget5s, falsificare ricevute o trattare SIGTERM come cleanup.

Readback21:15:24: root esatta
`/opt/nemesi-releases/1791219315548914929-57f3e5e100ec6605`, stato
`host_committed`/non healthy, misura9456entry/554994438byte PASS; quattro
config hash-identiche, agent/trial inattivi, worker993751 e cgroup agent
assenti. Quiescence attuale non prova rilascio/reset HW precedente.
Guard209byte, owner999, boot `c313a327-169e-498b-925b-d0fbdfc61ac8`, SHA
`afd16cc44aa4025629b93def326237dfa7ecbf667775c49fe8acf0241363c299`, reason
`provider_release_unconfirmed`; nessuna cancellazione o modifica del corpus.

### Punto di ripresa sicuro

Richiesto un **power-cycle fisico confermato**. Il tool standalone
[`recover_capture_guard.py`](../tools/recover_capture_guard.py),
[procedura](../tools/README.md), richiede reset reale di camera/acceleratori,
nuovo boot diverso, owner tutti fermi, lock maintenance/update canonico,
incidente e digest esatti. Conserva guard/receipt tramite archivio CAS, non
interpreta un reboot SSH come reset HW. Non attestare un power-cycle non fatto.
Poi completare il recovery/finalizer canonico della root già firmata, senza
ricompilare o rilanciare invariato il deploy FAILED; verificare postflight,
progresso capture/Tracker/Recorder/Registry e qualifica>=2700s applicabile.
Di notte13FPS resta continuità, non qualifica30FPS. Goal non chiuso.

Prove bounded private nel pack `broker-parity-20261005.drGkFjGa`:
`test-board-corrected-event-sandbox-r1.log`, `test-board-corrected-updater-progress-r15/r16.log`,
`test-board-corrected-final-service-failure-r1/r2.log`,
`test-board-corrected-terminal-boundary-r2.py/.json`, readback transazione r2/r3 e
misura terminale. [Rilascio, identità e limiti](../../../improvements/FARO_FAP_SIGNED_CANDIDATE_2026-10-05.md#esito-correttivo-r2--trial-pass-postflight-canonico-failed).
Nessun nuovo soak, kill, retry, modifica OS/rete/profilo/soglia o guard bypass.

### Regressioni pre-readiness, senza SDK o storage

Readback21:19:01 conferma stesso boot/guard SHA/root, agent ancora fermo:
nessun reset fisico o recovery attestato. In attesa della conferma, estesa
la fixture C `native/tests/worker_signal_test.c`: un exec figlio riceve
SIGTERM in un costruttore prima del main; il parent verifica uscita per
segnale, non receipt0/64. La prova è distinta dagli handler persistenti
già coperti dopo l'installazione. Nessuna pressione SD, camera o SDK reali.

Nuova regressione Go `TestNativeFrameExecutionRejectsEarlyTermination`:
SIGTERM reale su child senza handler, Wait raccolta, `Release` conserva
ErrUnavailable anche al replay; gruppo vuoto non diventa conferma HW.
Suite decoder nativa completa ASan/UBSan PASS (`OK_NATIVE_DECODER_PROTOCOL`),
lifecycle Go con race/count3 PASS1,047s. Runtime e budget invariati; nessuna
correzione causale dello stallo o nuova qualifica dichiarata.

Prove private `native-pre-main-signal-regression-r1.log` SHA256
`19180c8678fbf44611c342f88a7c19eac2dee08c58e45d86f60847dce37f0fc5`
e `native-early-termination-go-race-r1.log` SHA256
`a330c2af3bd897dfa7c64458d2a43ffc3d821750c0cecdbdce6ed6520079990b`.
Build RAM propria rimossa a test concluso, nessuna build in Git. Non rifirmare
o distribuire per queste sole prove. Per una correttiva del confine pre-main
servono fase causale e protocollo di cancellazione prima dell'uso SDK provati:
prefault/allungamento del timeout non sono, da soli, un contratto di rilascio.

## Stato aggiornato dopo recovery e prova mirata, 20:43 UTC

Secondo riavvio controllato eseguito alle20:33UTC, dopo misura esatta PASS,
assenza di transazioni/inhibitor e fingerprint delle quattro configurazioni
invariate. Nessun nuovo deploy, modifica OS o reset dei dati. Boot
`6b4a5b78-9439-4aa1-8c26-7cf5244b804c`, supervisor3311/invocation
`b2c8486ea1804213881e8c53a2dddcf6`, broker3459, stessa root`6f925237`.
Il primo avvio della sonda e' fallito prima dell'avvio del supervisor; sonda
riavviata dopo la readiness, senza riavviare di nuovo Nemesi.

Prova reale mirata alle20:36:48UTC: un solo SIGTERM via pidfd al worker3638,
con exe/parent/boot/invocation e handler verificati. Rilascio SDK confermato,
exit0; broker avvia epoch2/worker4276 alle20:36:51UTC e Tracker si riconnette.
Il motivo `child_exit_without_terminal_reason` in questa prova deriva dalla
terminazione esterna intenzionale, non da un nuovo guasto spontaneo.
La sonda osserva a40ms handler TERM assente, a291ms presente: distingue davvero
prima/dopo main; NON prova che la precedente attesa di5s fosse nel loader.
Anche una nuova misura completa della release con pipeline attiva passa senza
riprodurre la quarantena. I due incidenti precedenti rimangono aperti.

Secondo confronto mirato alle20:46UTC: una verifica completa della release in
lettura viene sovrapposta a un SIGTERM cooperativo al worker4276. Misura PASS
in14,511s, rilascio SDK confermato alle20:46:26 e nuovo worker6742/epoch3
alle20:46:29. Nessun nuovo timeout bootstrap. Quindi il solo carico di lettura
della misura NON basta a riprodurre la causa; niente terzo tentativo identico,
niente modifica speculativa a startup/cleanup o quarantena.

Readback autenticato30s dopo il recupero: CPUhost23,01–25,19%, I/Owait0,48–3,95%,
temperature80,17–80,61°C, Tracker11,77–13,39FPS, recorder in avanzamento,
rawqueue0/drop0, Registry non stalled/integrity0. Log22:42CEST: una nuova
osservazione elaborata, corpo rifiutato per bassa confidenza e nessun volto
SCRFD. Prova di elaborazione, NON identificazione di Salvatore o qualifica ottica.
La camera rimane a circa13FPS effettivi su30 richiesti. Nessun soak positivo.

Sonda read-only in RAM, durata massima1800s, unit
`nemesi-startup-probe-4FYpoU57`, archiviazione finale nel job persistente
`roi-rounding-20260916.4FYpoU57`. Non apre camere e non invia segnali;
termina prima se intercetta l'uscita precoce di un nuovo worker.

Diagnostica permanente **solo nel sorgente, non ancora distribuita**:
`bootstrap_observation_linux.go` fotografa stato, thread, handler/maschera TERM
e wait channel prima della cancellazione per bootstrap fallito. Input proc
limitati e log sanitizzato; indisponibilita' resta esplicita. Nessuna nuova
ricevuta di rilascio o modifica a deadline/quarantena. Test dedicati e suite
completa broker Go race PASS (27,311s). Non fare un deploy solo documentale.

## Incidente delle20:15UTC: video nuovamente fermo

Anche il successore`6f925237`, comprendente il fix ROI ARM64, ha passato
promozione/postflight e misura esatta9360entry/553725857byte, preservando tutti
gli hotfix e le quattro configurazioni. Non ha passato la verifica successiva:
timeout routing durante la misura file, poi brokerdegraded anche a verifica
file conclusa. Questi fallimenti restano validi; nessun soak positivo.
Alle20:10:56UTC native11371 ha registrato `analysis_send` wall377756us/CPU70661us,
poi discontinuita'H265 UVC2173->2176. Il successoreepoch2 parte20:11:01 e fallisce
20:11:06 per mailboxreadytimeout5s, SIGTERM/stderr vuoto. **Sigaction e' realmente
installato nei binari, ma questa correzione non basta a risolvere lo startup**.
Readback autenticato20:15: TrackerPID0/starts2/exits2, broker11134degraded,
Registry11133running senza input, recorder11209senza nuova pipeline; IPC0,
temperature68–69°C dovute anche al video fermo. Nessuna rimozione della quarantena.
`Launch.Deliver` invia metadata senza conferma dal programma nativo: il log
mailboxready non dimostra che il processo sia arrivato al main. Da distinguere
con osservazione all'avvio: attesa del loader/costruttore SDK prima degli handler
vs handler modificati dopo l'installazione. Non serve un altro retry identico.

## Evidenza live del 16 settembre

- Release di sviluppo `9c9631bdf035090a599d40b988ca478c071e9dfa00a325369b7b90b790fb346a`,
  supervisor PID350285, invocation `3dc76f318416419b93a4e6e5892f5414`.
- Otto epoch camera tra10:45UTC e14:29:55UTC: sette terminano per discontinuita'
  H265, una per `output_send:io_deadline`, tutte con exit64 e rilascio confermato.
- Alle14:28:11UTC la nuova diagnostica misura `decode_and_admission`
  wall4196855us/threadCPU1485us; poi sequenza UVC80406->80410. Localizza una
  lunga attesa, non dimostra ancora quale chiamata decoder/driver ne sia causa.
- Alle14:29:58UTC parte epoch9/PID402359. Alle14:30:03 fallisce dopo5000ms
  in `source mailbox ready`; `Wait` riporta `signal: terminated`, stderr vuoto.
  La mancata ricevuta di rilascio produce `sdk_release_unconfirmed` e il broker
  mantiene la quarantena. Non convertirla arbitrariamente in successo.
- Readback autenticato alle18:54:25 e18:54:35UTC: Tracker PID0/non running,
  starts11/exits11; recorder video_frames200297 invariato, archive idle;
  Registry running/worker_stalled=false/queue0 ma senza pipeline produttrice.
  Ultimo ingresso video14:29:55.586UTC. CPU termica64,6–65,2°C, DDR65,3–65,8°C.
  IPC authentication/validation failures0. Il vecchio `frame_flowing=true`
  residuo del Tracker NON prova un processo vivo. Il check broker canonico
  rifiuta correttamente lo stato degraded; nessun soak positivo dichiarato.

## Difetto riproducibile trovato nella terminazione

Il binario realmente installato importa `__sysv_signal`, non `sigaction`.
Con `_POSIX_C_SOURCE=200809L` e `-std=c11`, la registrazione tramite `signal()`
ha handler one-shot: un fixture isolato muore al secondo SIGTERM (segnale15)
senza poter consegnare la ricevuta di cleanup. Non e' ancora prova che oggi
PID402359 abbia ricevuto due segnali o che questo spieghi il suo timeout.

Correzione **installata su test board, qualifica continuativa aperta**: helper POSIX `worker_signal.h`
con `sigaction`, senza SA_RESETHAND/SA_NODEFER/SA_RESTART, adottato dai tre
worker V4L2, HBN e decoder. Fallimento dell'installazione resta esplicito prima
dell'inizializzazione hardware; nessuna modifica a timeout, gate, auth o DMA.
Fixture ripete TERM/INT/PIPE e verifica uscita controllata; integrato nel gate
native decoder. Suite decoder ASan/UBSan, HBN fanout, contratto V4L2 e lifecycle
broker sotto Go race PASS. Prima compilazione segnalava include stddef mancante,
corretto prima del rerun completo positivo.

## Stato e prossima azione

Riavvio controllato autorizzato dall'utente ed eseguito il16settembre circa
19:17UTC, senza reset dati o modifica configurazioni; deployed board invariato.
Preflight19:15:57UTC: misura esatta PASS9359entry/553724277byte, nessun inhibitor
o job systemd, configurazioni fotografate; richiesta sotto lock update canonico.
Alle19:20:52UTC confermato nuovo boot `5e967f8a-5a67-4334-a19e-0cfa3ce7bc2c`,
supervisor3283/invocation`e7d7ea8d10ae4ac78a698df306804c3a`, stessa release.
Il primo timeout SSH durante la ripartenza non era un rifiuto credenziale.
Avvio servizio non equivale ancora a pipeline funzionale o qualifica.
Readback autenticato successivo di30s: capture child3550 vivo, Tracker
13,18–13,38FPS, CPU host27,18–29,44%, I/O wait3,29–9,24%, temperature78,17–80,17°C;
release e quattro fingerprint configurazione invariati. Log nativi: circa13FPS,
attesa camera media65,20ms, decode1,02ms e lavoro perframe11,97ms. Non e' una
prova di limite FAP/decoder a13FPS: e' in prevalenza attesa dell'ingresso USB.
V4L2 frontvideo0 riporta30FPS richiesti, auto_exposure3/aperturepriority e
whitebalance automatici; lettura dynamicframerate restituisce EIO. Non alterati
autoesposizione, FPS o controlli per forzare un PASS; causa ottica/firmware della
cadenza effettiva ancora da distinguere. Nessuna nuova acquisizione concorrente.
La correzione segnali ha superato build onboard/promozione misurata/postflight;
resta la qualifica e non chiude da sola pause decoder, ROI o stallo raw recorder.

Diagnostica aggiunta nello stesso candidato: decoder misura separatamente mutex
di ammissione, dequeue/queueinput, copia accessunit e attesa output, con wall e
CPU del thread, massimo1report/s oltre100ms, stato del solo submitter. Fixture
SDK sintetica con150ms nel dequeue identifica esattamente quella fase e verifica
il rate limit. ROI segnala i campi divergenti (geometry/provider/copy_evidence),
senza payload o provider non fidato; validazione esatta invariata. Collector soak
aggiunge contesto dello stallo raw senza modificare la soglia5s. Test locali:
43SDK PASS, decoderASan/UBSan PASS, HBNfanout/V4L2contract PASS.

Build nativa isolata iniziata19:29UTC in
`/var/lib/nemesi/state/operator-backups/signals-20260916.WSxfvVsO`,
unit`nemesi-signals-build-WSxfvVsO`; pianoSHA
`ada54c952307e42d26df41cf0e54865d559419413cbfec25b25827a742af0b40`.
Delta confrontato con tutti i sorgenti installati, preserva gli altri hotfix.
Build/test onboard PASS, massimo84,473°C; verificato `sigaction` e assenza
`__sysv_signal` in tutti i tre binari. Promozione PASS, manifest
`f2bd393dd95ec2a24377c010871d21bc79578f60adab6f459dfbfa77c9af226c`, root
`dev-1789244163760113403-f2bd393dd95ec2a2-be6f37c6fd4e`.
Postflight PASS19:48:28UTC, supervisor7855/invocation
`ba9c9f127bc0457cbc7c28d0d234aa06`; quattro configurazioni preservate e misura
esatta PASS. Readback autenticato30s: Tracker13,10–13,29FPS, CPUhost25,85–27,75%,
temperature77,98–78,17°C, recorder avanza1770->2055frame, rawdrops0/code0,
Registryrunning/nonstalled/integrityerrors0. Non prova di nuova identificazione
facciale ne' soak45min/FPS>=18. Nessun gate modificato.

Durante questa promozione e' stato riprodotto anche il
[difetto di arrotondamento ARM64 dei ROI](2026-09-16-roi-arm64-fused-padding-rounding.md).
Quella correzione Go non fa parte del manifest`f2bd393d`: segue separatamente,
senza modificare retroattivamente il payload o le evidenze di questa transazione.

Le due retirement ROI per mismatch transform/provider sono state seguite dal
recupero Tracker, non dal blocco silenzioso di sette ore del15settembre. Rimane
da isolare quale campo differisse: la diagnostica attuale aggrega geometria,
provider e copy evidence. Non rilassare la validazione esatta per far passare ROI.

Traccia recorder13:29:45–13:30:30CEST conclusa regolarmente, tutti i thread
sganciati dal tracer; fsync massimo5,104088s, nessuna write sopra100ms osservata.
Non riproduce lo stallo raw precedente e non costituisce root cause ne' soak.
Evidenza privata persistente sul device:
`/var/lib/nemesi/state/operator-backups/reliability-20260916.vNfovhJz/recorder-syscalls.log`.
Controller, fixture e log locali:
`<private-workspace>`.
