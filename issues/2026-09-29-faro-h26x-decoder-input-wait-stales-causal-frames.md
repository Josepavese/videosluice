# test board: attesa input decoder H26x blocca il percorso condiviso

Stato: **OPEN — finding localizzato, correttiva da qualificare**.
Ambito: consolidamento corrente N2/N5/N6, non backlog laterale.
Correlato alla [qualifica health/Registry](../../setup/issues/2026-09-29-test-board-supervisor-health-timeout-night-soak.md).

## Prova fisica del29 settembre

Registry ottimizzato installato con promozione misurata,11test nativi,
postflight e verifica indipendente PASS. Il successivo soak invariato2700s
test board r2 fallisce alle14:56:23 CEST, dopo circa82s: frame sorgente del Tracker
vecchio4.334ms al momento dello stato, limite originale3s. Non correggere
questo risultato aumentando la soglia o scartando il campione.

Root `dev-1789244163760113403-4293280f91f71da6-2e153e068532`,
manifest `4293280f91f71da6d210e288bfdd7acfc319a5329c2733c2f8c315aa0b81c32b`.
PAL privato `/var/lib/nemesi/state/operator-backups/registry-idle-20260929-r2/`:
`health-soak-failure.json`, `health-soak-framing.jsonl`,
`decoder-gap-evidence.log` (12righe bounded, nessuna immagine).
Unit `nemesi-registry-health-soak-20260929-r2.service` FAILED exit1.

Sequenza dello stesso broker1225323/bridge1225531, epoca1:

1. 14:56:17: circa30/20/15FPS e gap massimi44/82/76ms, nessun drop fanout.
2. 14:56:19: `phase=dequeue_input_buffer wall_us=1231239 thread_cpu_us=1297`;
  `decode_and_admission`1.232.282us wall/2.342us CPU. L'attesa è nel feed
  del decoder, non un secondo di lavoro CPU del thread.
3. Subito dopo: discontinuità H265 USB17881→17916, delta35;
  `action=rebuild_reference_gate`. Non attribuire automaticamente il salto
  a un guasto della camera: il capture loop era bloccato a valle nel decoder.
4. 14:56:21: nuovo IDR, attesa dell'origine decodificata esatta/cadenza stabile.
5. 14:56:22: analysis7,75/archive6/live12,75FPS, degraded_for_ms4000.
6. 14:56:23: gate frame age FAILED. Health completo9.257byte/newline/EOF,
  durata4,967ms; nessuna riproduzione del precedente bug EOF/logger.
7. 14:56:30: recupero automatico di cadenza30/20/15, NON PASS del soak.

Dopo il FAILED: MainPID1225200/invocation81191bf6c075469eaadbb0a25c2e1a05,
NRestarts0, stessa root. Temperatura letta86,329°C durante la diagnosi,
non una misura dell'istante esatto del blocco. Nessun nuovo restart/fault/retry.

## Confine software confermato e limiti causali

`native/rdkx5_decoder_runtime.c` identico tra Git e sorgente installata,
SHA256 `9bcf488338237216e328861a25f24d5c35bcb847fc3a6d2290d6d13b3885f60a`.
Il feed usa `hb_mm_mc_dequeue_input_buffer(...,2000)`; anche queue/ritorno
buffer hanno2000ms. Il timeout breve passato da `decode_to_nv12` governa
l'attesa output, NON queste operazioni input. Il feed H26x è chiamato nel
percorso condiviso capture/decode/admission: il wait osservato può bloccarlo.
Non abbiamo ancora provato perché il pool/driver abbia atteso1,23s:
crediti/buffer output, lock/scheduling, memoria/I/O o driver restano da distinguere.
Non è dimostrato un guasto SD, un leak, CPU satura o una regressione causata
dallo storage Registry; i binari video sono stati preservati nella promozione.

## Prossima correttiva, senza tentativi ciechi

- Riprodurre il wait input con lo stub nativo e verificare crediti/rilascio
  dell'output prima di scegliere la correzione. Lo stub permette input delay/
  block ma ignora il parametro timeout: una prova fixture non certifica il
  comportamento SDK. Aggiungere osservazioni bounded di budget richiesto,
  SDK code, ownership e crediti alle fasi realmente mancanti.
- Dare al feed un budget esplicito/coerente e, se necessario, separarlo dal
  drain della cattura con code RAM possedute e limitate. Non sostituire
  semplicemente2000con0/2: bisogna qualificare il comportamento SDK su busy,
  gli input non sottomessi e quelli di ownership ambigua.
- Preservare origine/epoca/ticket e lease, stop/release hardware verificati.
  Non scartare P-frame arbitrariamente senza resync IDR e non mantenere crop
  vecchi come se fossero nuovi. Tenere live passthrough indipendente dal feed
  dove possibile, senza separare artificialmente l'archive dalla sua origine.
- Contratto standalone nel video-broker; verificare percorsi H264/H265,
  JPU/MJPG e acceleratori supportati senza policy ad hoc test board, logiche Tracker/
  Registry nel broker o fallback software. Nessun cambio profilo/FPS/soglie.
- Solo dopo causa/correttiva/regressioni: qualifica integrata test board con gate
  originali, un solo nuovo soak e MP4 nuovo chiuso/decode. deployed board spento intatto.

Il monitor terminale è PAUSED, non il goal. Conservare anche tutti i FAILED
precedenti. Non chiudere l'issue perché la cadenza si è ripresa da sola.

## Diagnosi passiva successiva — 29 settembre17:33–17:38 CEST

Richiesta utente: distinguere inefficienza da SD e correggere i difetti provati.
Nessun deploy/restart/fault, secondo lettore camera, cache drop, cambio profilo,
durabilità o soglie. deployed board spento intatto. Nuove sonde mantenute in
`src/setup/tests/capture_recovery_20260928/`: `probe_decoder_wait.py`,
`summarize_decoder_wait.py`, `profile_sqlite_cpu.py`. Prove root-private:
`/var/lib/nemesi/state/operator-backups/decoder-observation-20260929-r1/`
e `decoder-observation-20260929-r2/`; nessuna prova nella release attiva.

- Osservazione300,088s,2995campioni main a100ms/300campioni sistema a1s,
  con pin pidfd su supervisore/broker/native e binding della fotografia
  autenticata. Evidenza in RAM durante la raccolta, persistita solo alla fine.
  CPU osservatore9,620s (circa3,2% di un core), non carico runtime.
  JSON SHA256 `5e2ba7b5988a06c9d756604359d9e70ecc300969e38009f65419ff90d84ee0a1`.
- Cinque metriche native da60s: **30,00FPS**, decode medio0,82–0,85ms,
  massimo4,30ms; gap cattura massimo47,92ms. Nessuna fase lenta input
  riprodotta. Native main senza D, zero major fault/read_bytes nel periodo.
  Wait D di altri thread `hash_sendmsg`/`osal_completion_wait` non sono
  automaticamente page wait SD o un guasto: acceleratori/SDK restano distinti.
- CPU, riferita a UN core (CLK_TCK100): native circa36,6%, Tracker35,6%,
  Registry12,1%, **Captioning98,8%**; CPU aggregata31,6%, iowait3,0%.
  Temperature84,424–86,720°C sulle due zone. Nessuna prova di leak o stabilità termica lunga.
- Registry4,297GB di rchar ma soli0,758MB di read_bytes; Captioning2,596GB
  di rchar ma5,759MB di read_bytes. Gran parte delle letture viene da cache,
  NON quei GB letti fisicamente dalla SD. Write_bytes Recorder75,534MB,
  Registry1,954MB, Captioning29,225MB con23,790MB cancellati: non sommare
  questi contatori come byte durevoli arrivati al supporto.
- mmcblk1:42,586MB letti/111,251MB scritti,6627richieste, busy22,6%,
  attesa media request451,7ms (comprende accodamento, non test di salute SD).
  Reclaim kswapd25.557pagine/387major fault globali; zero swap-in/out o
  allocstall diretto. La latenza storage resta reale, ma nel periodo non
  coincide con uno stallo osservato del decoder. Nessun nuovo errore MMC/
  EXT4/OOM emerso dalla lettura bounded del journal kernel warning+.
- Profilo CPU passivo20s/49Hz, senza ptrace/stop/segnali ai servizi: thread
  Captioning **1226507/Tgid1225288** dominante,71,76% campioni nel binario
  Python,11,50% kernel,4,24% libc. Non attribuirlo per deduzione alla query
  SQLite corretta: il frame Python preciso resta da isolare. Perf raw207.868byte
  conservato privatamente in r2 dopo la raccolta, SHA256
  `17fafda8e1ddc349a0ed63566364ded20cab844d1c6377e0ae5850e15511ddc9`.

Verifica finale autenticata alle17:47:21 CEST: stessa root/supervisore,
NRestarts0, quattro config hash invariati; recorder `recording`, ultimo sync
4.326ms; Registry running, coda0/stalled=false/integrity_errors0, Tracker
frame age29,826ms. Native1234991/Tracker1234992 diversi da quelli del FAILED
r2: il Tracker pubblica starts2/exits1, quindi c'è stato turnover prima della
raccolta, NON una qualifica della stessa epoca del soak. Non nasconderlo o
riclassificare il precedente FAILED. Nessuna misura completa della root
ripetuta durante la raccolta né nuovo soak avviato.

**Correzione software dimostrata, solo nel sorgente:** Captioning aveva ancora
il SUM/CASE inbox senza filtro sugli stati: scansione inutile dei completati.
Filtro degli stati non terminali attraverso l'indice già esistente, risultato
identico, nessuna migration/cache, record cancellato o cambio durabilità.
Fixture20.000completati+4attivi:260.071→104istruzioni VM per la query,
non una riduzione della CPU totale; test contatori vuoti/transizioni/dati
preservati e read-only PASS. Suite Captioning223PASS, decoder ASan/UBSan
`OK_NATIVE_DECODER_PROTOCOL`,5regressioni sonde PASS, tutti in RAM host.
Fix NON ancora installato: evitare una clonazione/restart completi soltanto
per questa micro-ottimizzazione, mentre la causa del wait input resta aperta.

Conclusione non binaria: inefficienza software concreta corretta nel codice,
hotspot Python e latenza storage osservati, **SD fisicamente guasta non provata**.
Residui nello stesso consolidamento: identificare lo stack del thread hot,
correlare una vera ricorrenza del wait con I/O/page fault/crediti SDK e motivare
il fix del feed. Solo poi promozione qualificata/soak invariato; eventuale A/B
del supporto deve preservare SD originale/dati e workload identico. La prova
passiva senza incidente non sana FAILED/goal, né autorizza manutenzione OS.

### Turnover precedente localizzato — non soltanto input decoder

Lettura retrospettiva bounded del journal JSON, senza ripetere fault o raccogliere
foto: `pre-observation-turnover.json` nel PAL r1, timestamp journald preservati.
Stesso vecchio child1225531 prima della nuova finestra passiva:

- 15:23:23,525 CEST: `archive_transform_encode_send`604.520us / CPU2.356us,
  salto16frame, resync.
- 15:24:29,909: `metrics_log`1.735.632us / CPU332us, salto50frame, resync.
- 15:25:41,759: `archive_transform_encode_send`6.155.896us / CPU3.067us;
  salto182frame, `retire_source_before_publication`.
- 15:25:42,041: `capture_epoch_ended`, reason
  `interframe_capture_discontinuity:h265`, exit64. Non è una nostra mutazione.

Questi wait non sono CPU impegnata a codificare6s. La sola ottimizzazione del
dequeue input non copre tutti i finding. Il percorso `metrics_log_if_due`
esegue fprintf sincroni; lo snapshot publisher è invece una lettura atomica
bounded, senza mutex. **Rettifica verificata successivamente:** `scanStderr`
chiama il logger standard prima di alcuni parser, ma `cmd/video-broker/main.go`
ne ha già impostato l'output su `internal/diagnosticlog.Writer` bounded/asincrono.
Gli hash di entrambi i sorgenti coincidono Git/installato. Non aggiungere un
secondo logger Go né presentare quel call site come prova di sink sincrono
in produzione. Il `fprintf` nativo rimane invece un confine diverso. Non abbiamo lo
stack/syscall del momento: distinguere pipe/logger, descheduling e attesa SDK;
non dichiarare ancora provato quale write/driver abbia bloccato l'archive.

Correttiva strutturale residua: drain stderr separato dall'I/O diagnostico con
coda RAM bounded, contatori di perdita/coalescing e parsing delle receipt
autenticate sincrono indipendente dal logger; nessuna receipt hardware basata
su un log differito/scartato. Un solo worker lifetime, niente goroutine in
crescita a ogni epoca; provare logger bloccato e pipe piena senza SD. Per archive
servono checkpoint distinti resize/encoder/queue-write prima di scegliere il
fix di backpressure/crediti. Conservare lease/epoche/H26x e stop SDK verificato.
Nessun cambio del logger/encoder runtime eseguito da quella diagnosi.

## Correttiva nativa e Captioning — 29 settembre, sera

Richiesta utente: correggere i finding e preferire acceleratori hardware reali.
`diagnostic_pipe.h` separa le sole metriche native non critiche dal logger
stdio: open file description indipendente su FIFO, O_NONBLOCK, record<=PIPE_BUF,
un solo tentativo di write, scarti contati, niente heap/thread/retry. Non modifica
i flag di stderr del vendor, errno o handler globali; SIGPIPE della propria
write viene gestito senza consumare un segnale già pendente. Sink file regolare
non ammesso nel media loop. Ricevute SDK, controllo e release restano affidabili,
non convertite in telemetria lossy. Coperti anche i riepiloghi qualità/tone.

Pipe piena/chiusa, recupero, oversize, flag originali, file regolare e errno:
regressione reale senza SD, ASAN/UBSAN PASS; gate decoder completo PASS.
Archive ora distingue transform, encode e mux-write. Encoder persistente
osserva dequeue/copy/queue input e dequeue/copy/queue output, senza I/O log
nel metodo encode e senza modificare timeout/buffer/ownership. Fixture SDK
output150ms localizzata e non attribuita al frame successivo; suite native media
e contratto warmup PASS. Questo NON prova ancora la causa del wait6,15s.

Campionamento Python20s/25Hz `py-spy0.4.2 --nonblocking`, senza stop/locals,
RAM durante acquisizione, binding health/pidfd/root verificati. PAL privato
`captioning-python-20260929-r1`, stack SHA256
`ae89b6ebc1d08ced9fda73af54dadf42b48d7b421aced566d953bac2433ee17d`.
Mostra ripetute scansioni di tutti i pin in `_finalize_successful_inbox` e
warmup embedding; i sample IPC/select non vanno letti come percentuali CPU:
il profiler nonblocking può includere thread in attesa. Finding/correttiva
Captioning nella [issue dedicata](../../event-captioning/issues/2026-09-29-terminal-pin-cleanup-corpus-scan.md).
224test Captioning PASS, senza cambio schema DB o cancellazione dello storico.

Prima preparazione live `media-captioning-20260929-r1`: fixture native PASS,
SDK build in pausa termica (cc1 inT; runtime invariato), poi annullata soltanto
la unit di build posseduta. Passaggio offline: r2/r3 rifiutati dal lock ancora
detenuto da flock della preparazione, prima di misura/quiesce/stop; conservati
nei journal. Risolto il cancel del solo job di build con MainPID/invocation
verificati e opzione systemd supportata `--kill-who`, senza toccare processi
Nemesi. Non confondere queste fasi con soak falliti o nuovi deploy.

PAL candidato `media-captioning-20260929-r3`, tar verificato
`f90a4a8a0d0184552e7abcebd4f48895d3229d9457af9e7c1766d2c3393b7372`;
unit `nemesi-media-captioning-activate-20260929-r3b.service` per singola
promozione offline. Guard/drain/cooling originali, runtime main/altri binari e
quattro config preservati; SDK build in RAM, permessi runtime conservati.
Esito installazione/postflight e qualifica ancora da verificare. Nessun PASS
soak, guasto SD, soluzione completa dei wait o chiusura goal dichiarati.
deployed board spento intatto; tutti i FAILED precedenti conservati.

## Nuovo evento integrato — 29 settembre19:12–19:15 CEST

La candidata nativa/Captioning r3 è stata promossa con misura esatta ma ha
fallito il postflight per drift schema IPC `pin_cleanup` nel binario Go
incorporato. Correzione misurata separata `caption-contract-20260929-r1`,
manifest `b9362745fcd0e02373410936c28cabce4b2ff5a7ab76eb1eee7ce06c7158525d`:
test Go ARM, postflight, root/tre file/sei binari preservati/quattro config e
misura9399entry **PASS**. Nessun cambio sorgente, HW, soglie o deployed board.

Il soak `nemesi-caption-contract-soak-20260929-r1.service` è **FAILED alla
baseline**, non un PASS parziale: alle19:15:38 il Task6 aveva starts2/exits1
e il collector autenticato ha rifiutato la stessa invocation. La causa del
turnover è documentata nel journal alle19:12:54–57: `analysis_send`474,695ms
wall ma5,975ms thread CPU; V4L2 H.265 salta sequenza1005→1009 (delta4),
resync IDR scade in2s, broker chiude l'epoca, Tracker riceve peer retired
`-6` ed esce per nuovo binding. Il broker riparte poi automaticamente,
registrazione/Tracker proseguono; questa ripresa non cancella il FAILED.

La pubblicazione analysis usa **già** `source_publisher.c`: due slot RAM
immutabili, submitter senza attesa del worker, nessun ACK per-frame, copie
max3,93ms e lavoro publisher max44,77ms nella prima epoca. La fase larga
`analysis_send` non basta ad attribuire475ms al publisher o allo SHA.
Serve scomporre autorità/digest/copia/scheduling e correlare il thread con
I/O/page wait; non duplicare una coda già presente. Il loop fisico resta
seriale (capture, decode, transform, publication, archive): anche uno stallo
non-CPU a valle può svuotare i buffer V4L2 e perdere sequenze H.265.

Misura read-only successiva sullo stesso runtime (soak già terminale):
`iostat -dx -y 5 6 mmcblk1` ha mostrato write await fino a3.508,92ms in una
finestra5s con solo1.020kB/s scritti, altre finestre695,13/1.829,89ms e
utilizzo fino92,4%; `vmstat` ha toccato36% iowait ma zero swap-in/out.
`Dirty` circa1.256kB, `Writeback`32kB, memoria disponibile1,4GB: non è
semplicemente l'intero clone ancora sporco in cache. Il journal kernel
bounded dalle18 non mostra errori MMC/EXT4/timeout. È forte evidenza di
latenza dello storage sotto lavoro normale, **non prova della causa di quello
specifico stallo alle19:12 né prova di SD fisicamente difettosa**. Per
distinguere software/fsync da supporto serve confronto A/B conservativo con
SD buona oppure telemetria correlata per richiesta/processo; preservare
l'originale e i dati, non abbassare durabilità o gate per far passare il soak.

Prossima scelta tecnica: qualificare una vera separazione del dequeue V4L2
dal lavoro per-frame tramite ownership di buffer compressi RAM bounded,
resync H.265 e backpressure espliciti, riusabile nel video-broker per tutte
le piattaforme, senza spostare logiche Tracker/Registry o fingere acceleratori.
Prima della modifica misurare le sottophase e verificare il comportamento
con una SD sana; una sola nuova candidata dopo causa/regressioni, non un
restart/soak ripetuto alla cieca. deployed board OFF intatto.

## Soak r3 del29 settembre sera: output replay distinto dal wait decoder

La promozione misurata Captioning r3 ha superato il postflight, ma il soak
integrato originale2700s/0,5s è **FAILED alle22:02:12 CEST** dopo baseline
autenticata. Nel PAL privato
`/var/lib/nemesi/state/operator-backups/caption-pin-lookup-20260929-r3/`
restano `health-soak-failure.json` e `health-soak-framing.jsonl`; health
completo9244byte/newline/EOF, ricezione3ms. Broker epoca1:
`output_commit_progress phase=validation origin_sequence=6451 ...`
`output_replay: invalid_contract`, poi retire capture ed epoca2. Prima del
fault i contatori publisher indicavano6390 submitted/published, nessun
replaced/busy/expired, copia massima3,6ms e worker massimo47,98ms.
L'evento non dimostra CPU saturata, errore SD, né la ricorrenza del wait SDK
decoder. Il recupero automatico non trasforma il FAILED in PASS.

Ipotesi strutturale verificabile: il ricevitore di due slot immutabili può
leggere uno slot nuovo mentre l'altro risponde busy; al poll seguente lo slot
prima busy può esporre un output completo più vecchio del massimo già
consumato. Il seriale per-slot cresce, ma `acceptVerifiedOutput` lo rifiutava
come replay strutturale e ritirava la sorgente. Il log originale non include
le due sequenze, quindi l'ipotesi non è ancora causalmente confermata su test board.
La candidata nel video-broker distingue sotto lo stesso lock un output
**strettamente più vecchio**, dopo verifica di header, autorità e SHA, e lo
conta come superseded senza pubblicarlo; sequenza uguale, corruzione e
autorità errata restano fatali. Log nuovo include sequenza e precedente
massimo nei failure e contatore superseded separato dai drop di pressione.
Regressioni Go per interleaving, equal replay, hash/epoch/authority e suite
`go test ./...` PASS sul host. Da compilare con toolchain ARM64/CGO1 installata,
promuovere misurato su test board e ripetere il soak invariato per confermare o
confutare l'ipotesi. Nessun cambio FPS, profilo, limiti o fallback software.

Promozione successiva su test board: primo PAL `output-supersession-20260929-r1`
**FAILED solo in preparazione**, perché le fixture C si aspettavano la
struttura `<PAL>/video-broker` mentre la sandbox le collocava sotto
`build-source`. Broker attivo mai fermato in r1; errore e log conservati.
Controller corretto e tracciato nel PAL r2. Test ARM64/CGO1 `frameingress` e
`cmd/video-broker` PASS (fixture native comprese), binario AArch64 compilato
dal toolchain installato. Payload e sorgenti SHA verificati; promozione
misurata/postflight **PASS**, manifest
`7767bfd4f5b8394c1772d94c0261d8623372c25565cdac025221f62f868a7ed1`,
root `/opt/nemesi-releases/dev-1789244163760113403-7767bfd4f5b8394c-315a3c8181dc`.
Main e quattro configurazioni preservati; NRestarts0 al postflight.
Soak originale2700s/0,5s avviato alle22:43 CEST nella unit
`nemesi-output-soak-20260929-r2.service`, baseline autenticata: Tracker2445,
archive2454, massima73,481°C. **PASS** dopo2700,118s/5400campioni,
zero overrun, picco77,487°C, stesso PID/invocation, Tracker +34956frame
(12,95FPS notturni), Recorder +34980frame, archive +34949scritture e zero
drop archive/raw. Report `health-soak-pass.json` nel PAL r2; misura finale
indipendente PASS (9399entry/554393548byte), root/manifest e quattro hash
config invariati, Registry running/non stalled, Tracker/Recorder avanzano.
MP4 chiuso `20260929-211000.mp4` nato dopo la baseline (21:10-21:20 UTC):
52214771byte, H.265 640x360, 599,554s/7767frame; SHA-256 dispositivo e
host `de0aafb00d67a57c0b297cb0c7231bdd6507be6860656bf124c3f6f70cdc59c3`,
ffprobe e decode completo offline a un thread PASS; copia temporanea in RAM
rimossa. Nessun secondo collector, retry o cambio dei gate; deployed board spento
intatto. Questo qualifica la continuità integrata della candidata development
su test board di notte, non prova l'assenza generale dei wait H.265, la pressione
SDK fisica, 30FPS diurni, una release firmata o la chiusura del goal.

## Verifica diurna read-only — 30 settembre

Stessa root `dev-1789244163760113403-7767bfd4f5b8394c-315a3c8181dc`,
supervisor PID1333717/invocation invariati, NRestarts0. La front è acquisita
H.265 a1920x1080@30; log di finestre sane mostrano live circa30FPS,
analysis20FPS, archive15FPS e backend decode/encode `native_sp`, resize
`native_neon`. Misura autenticata di30,033s alle10:06 CEST: Tracker
534frame/17,78FPS, archive450frame/14,98FPS, zero drop archive/raw,
Registry running/non stalled, temperatura massima82,128°C.

Il soak notturno PASS non si estende alla giornata: dopo la sua fine il broker
ha ritirato e recuperato tre epoche per fault H.265 alle00:01:12 e01:20:23
(`interframe_resync_timeout`) e alle10:00:13 CEST
(`interframe_capture_discontinuity`, sequenza V4L2 523008→523108).
Il Tracker ha seguito il retirement del binding e il supervisor lo ha
riavviato tre volte (task6 starts4/exits3); broker/Recorder/Registry restano
running. Prima dell'ultimo fault: `decode_and_admission` ~997ms wall/~2,4ms
CPU thread e `native_surface_publish` ~2417ms wall/~1,2ms CPU thread,
quindi attesa non spiegabile come solo calcolo CPU. Nella finestra kernel
10:00 non compaiono errori USB/MMC/EXT4; questo non esclude driver, hardware
o I/O intermittente. Nessun nuovo deploy, restart, fault injection o cambio
di profilo. Il gate diurno non può PASS sulla invocation con tre exit pregressi;
serve isolare il blocco nativo/camera e qualificare nuovamente con misure
comparabili, senza abbassare soglie né inferire una SD guasta.

### Localizzazione delle tre attese, senza nuova mutazione live

Telemetria già prodotta dal runtime prima dei tre retirement, correlata per
incident/child/epoch; non è un nuovo esperimento:

| CEST | Fase lenta misurata | Effetto | Contesto |
| --- | --- | --- | --- |
| 00:01:10 | VPU `dequeue_input_buffer` 1752,743ms wall / 0,986ms CPU thread | salto21 AU, IDR non arrivato entro il budget invariato di2s | `decode_max_ms=1783,26`, copia mailbox max4,47ms |
| 01:20:20 | VPU `queue_input_buffer` 342,917ms wall / 0,263ms CPU thread | salto4 AU, resync scaduto | `decode_max_ms=343,70`, copia mailbox max10,51ms |
| 10:00:12–13 | `decode_and_admission` 996,651ms e `native_surface_publish` 2417,482ms wall, ciascuna con circa1–2,4ms CPU thread | salto100 AU, retirement immediato | `decode_max_ms=7,82`; `write_main_max_ms=994,37` nella finestra19,4s; copia mailbox max14,21ms |

L'ultima fase `native_surface_publish` comprende ancora controlli di
autorità e submit, quindi il contatore della sola copia non ne localizza
l'attesa residua. Il passthrough live invoca `write_mux_frame`/`fflush`
sincronamente nel loop nativo; il lettore Go demultiplexa e dispatcha nello
stesso loop, anche se i singoli client hanno code bounded. Il massimo
`write_main` di circa1s è una misura della finestra, non prova che sia stato
il primo evento causale: occorre separare lettura del mux e lavoro downstream
o misurare il blocco preciso senza perdere ordine e integrità H.265/archive.

**Ulteriore verifica diurna alle10:11 CEST:** stessa invocation e release,
epoca4 ritirata per `interframe_capture_discontinuity` dopo un salto V4L2
19533→19619 (86 access unit). La telemetria dell'epoca misura
`decode_and_admission` 786,647ms wall/1,860ms CPU thread e
`native_surface_publish` 2147,677ms wall/3,262ms CPU thread; decode SDK
massimo4,36ms, `write_main_max_ms=784,84`, `capture_gap_max_ms=2937,81`.
Nel percorso H.265 diretto, `write_mux_frame`/`fflush` è dentro
`decode_and_admission`: il primo intervallo coincide quasi interamente con
una scrittura mux lenta, senza dimostrare ancora perché il lettore padre abbia
atteso. Il secondo intervallo è dentro `video_broker_frame_source_publish`,
mentre la copia nel publisher RAM era stata misurata separatamente; resta da
localizzare authority/health/submit o un'attesa di scheduling. Recupero
automatico in epoca5; alle10:34–10:37 il broker è tornato a live≈30,
analysis≈20, archive≈15FPS senza drop archive/raw. Un campione con il
validator canonico non può qualificare la stessa invocation: il task Tracker
6 risulta già riavviato/uscito, come previsto dal gate originale. Questo è
un quarto fault post-soak, non un nuovo PASS diurno e non prova guasto SD.

`sar` storico a10 minuti attorno alle10:00: SD `mmcblk1` await medio241,83ms,
queue15,92, utilizzo64,32%, CPU iowait14,04%, major fault2,62/s;
swap-in/out0/s, RAM disponibile circa1,48GiB. Nelle finestre dei due fault
notturni l'iowait era circa1–3% pur con await SD variabile93–264ms.
Questi aggregati non attribuiscono il singolo wait alla SD né dimostrano
guasto fisico. Il prossimo cambiamento deve proteggere il dequeue UVC dai
consumer/SDK che attendono, con ingress H.265 bounded, ownership/fencing
invariati e fail-closed se la capacità finisce; diagnostica per subfase e
prove di slow consumer, reorder, IDR e arresto SDK prima di una promozione.

### Isolamento mux e diagnostica FAP — installati, soak diurno FAILED

Nel sorgente Go il lettore del pipe nativo è ora separato dal dispatch con
una coda FIFO per epoca, limitata a256 record e24MiB di **allocazioni
trattenute** (non solo byte wire). Overflow o framing invalido ritirano
esplicitamente l'epoca; non si scartano in silenzio access unit H.265/archive.
La coda analysis esistente conserva la politica freshest-only ma ogni elemento
porta l'epoca, verificata sotto il lock del dispatch per impedire frame vecchi
dopo restart. Il lettore viene raggiunto e chiuso prima del retirement; un
client lento non possiede il lease V4L2. Nel provider FAP nativo i massimi di
wall/CPU thread delle tre sottofasi health, authority e submit sono registrati
in memoria e pubblicati nel riepilogo periodico/finale tramite la telemetria
non bloccante già esistente, senza nuovo `fprintf` per frame o in cleanup.
Non è un cambiamento di soglie, profilo, codec o fallback.

Prove host: `go test ./...` PASS con fixture Unix sotto `/tmp`; i primi due
tentativi con `GOTMPDIR=/dev/shm` fallivano soltanto per pathname socket Unix
troppo lungo, e gli stessi pacchetti passano con path corto. Test mirati del
lettore con dispatcher bloccato, quota fisica pooled, overflow e fencing
di epoca PASS; `go test -race ./internal/broker` PASS; fixture nativa
ASAN/UBSAN `TestNativeFrameCaptureMuxLifecycleAndRestart` PASS. La candidata
è stata compilata e testata su test board ARM64/SDK (`go test ./...`, build Go e
bridge nativo PASS) nel PAL privato `test-board-mux-20260930-r1`. La promozione
misurata ha installato il manifest
`4db63da10eb2e7cb167682d688e72aea8c6971c7b8c9f2a4d2666a7d1851bb1c`
nella root `dev-1789244163760113403-4db63da10eb2e7cb-d316c8ba1724`:
misura 9401 entry/554411034 byte, dieci payload verificati, quattro config
preservati, postflight PASS e supervisore ripartito con `NRestarts=0`.
Il soak diurno originale 2700s/0,5s è stato avviato alle12:23 CEST nella unit
`nemesi-mux-day-soak-20260930-r1.service`, senza fault injection, secondo
collector o variazione dei gate. Alle12:48:54 CEST la unit è terminata
**FAILED**, `CollectionTimingError: recorder status is stale` (limite
originale 5s). Il frame health finale era completo (9277byte, un newline,
EOF, 4,627ms); l'errore è nella validazione dello snapshot, non nel socket.
Nello stesso istante il Recorder ha loggato una scrittura storage di 8MiB in
4717ms senza errore I/O. La correlazione temporale non prova da sola che la
scrittura abbia bloccato la pubblicazione dello stato: il writer è asincrono,
mentre lo status gira in un loop separato con intervallo configurato a2s.
Occorrono timestamp di publish/IPC e prova di scheduling/lock prima di una
correttiva; non allargare il gate per ottenere PASS. Dopo il FAILED, un
campione autenticato singolo alle13:08 CEST mostra Recorder PID1515458
running/recording, status age1962ms, archive writing e una finalizzazione
fallita. La continuità successiva non sana il soak.

Diagnostica candidata nel sorgente Recorder, **non installata**: un evento
bounded `status_publish_timing` soltanto se il gap supera l'intervallo di
500ms o una fase snapshot/IPC supera250ms; riporta gap, lag del tick,
tempo snapshot, invio IPC e age dello status senza payload o dati personali.
Test normali/ritardo scheduler/snapshot/IPC PASS, suite Recorder e race mirata
PASS. Permette di discriminare se un nuovo stale origina nel publisher
oppure dopo l'invio al supervisor, senza alterare intervallo o gate.

Nella finestra prima del fallimento, i riepiloghi nativi mostrano sorgente
front a30,00FPS, analysis20 e archive15, senza timeout/corruzione di cattura;
nessun fault H.265 osservato in questa finestra. Questo supporta solo il
funzionamento locale dell'isolamento mux, non la qualifica diurna. L'efficacia sotto
camera/SDK fisici resta **non ancora dimostrata**. La coda Go affronta il blocco della
scrittura mux, non presume di risolvere l'attesa FAP/SDK di oltre2s. Prima
della qualifica finale: causa/correttiva del FAILED, nuovo soak e prova di un MP4 nuovo chiuso/decodificato,
più misura indipendente finale della root e delle configurazioni. deployed board spento
intatto. Report `health-soak-failure.json` e framing nel PAL privato
`test-board-mux-20260930-r1`; nessun retry, restart o deploy. Questa è una release
di sviluppo, non una release firmata. La distinta finalizzazione VOD fallita
è documentata nell'[issue Recorder](../../recorder/issues/2026-09-07-progressive-vod-non-monotonic-sample-finalization.md).

### Nuovo FAILED diurno r4 — 30 settembre15:21:48 CEST

Il candidato Recorder r4 e` installato/postflight PASS, ma il singolo soak
2700s/0,5s e` **FAILED** dopo circa4min16s per frame Tracker vecchio3509ms
al momento dello stato (limite originale3000ms); health completo in3,195ms.
Il riepilogo nativo successivo della stessa epoca mostra front30,00FPS,
analysis20, archive15, zero timeout/corruzioni e massimi decode3,32ms,
publish worker96,29ms. Non emerge in questa finestra un wait del broker
analogo ai fault H.265 precedenti. Il ritardo potrebbe essere nel consumo
Tracker/BPU/ROI/scheduling: la telemetria Tracker recuperata dopo il primo
riscontro localizza un'iterazione recognition3506ms e detector worker3189ms,
con salto successivo69frame; si veda la
[issue Tracker](../../tracker/issues/2026-09-30-test-board-detector-worker-stall-day-soak.md).
Non attribuire la causa al broker o alla SD. Report nel PAL privato
`recorder-vod-20260930-r4`; dettagli nell'issue Recorder. Questo evento
non confuta i vecchi fault H.265, ne` li chiude. Nessun nuovo tentativo,
profilo o soglia alterati; monitor terminale PAUSED.

Alle15:45:00 CEST, **dopo** il soak fallito, la stessa epoca e` stata
ritirata per `mux_dispatch_backpressure`, non per errore di cattura V4L2:
il riepilogo nativo15:44:59 mostrava30/20/15FPS, zero timeout/corruzioni,
`write_main/sub/archive_max_ms` sotto1ms nell'ultimo minuto. All'uscita il
broker riportava `last_sub_age_ms=7710` contro `last_frame_age_ms=328` e
`last_archive_age_ms=328`; la coda mux nuova ha quindi esaurito il suo budget
mentre il consumer analysis non avanzava, ma manca ancora lo stadio bloccato
preciso. Il broker ha avviato autonomamente epoca2 alle15:45:04 e recuperato
30/20/15FPS entro15:45:20, senza restart del supervisore. Non attribuire
automaticamente questo evento al FAILED Tracker delle15:21 o alla SD. La
prossima diagnostica deve distinguere lock/SHM/FAP, dispatch e scheduling
con tempo e occupazione della coda per stream; non aumentare la quota RAM o
scartare pacchetti H.265 per nascondere l'overflow.

### Distinto stallo della transazione FAP — 30 settembre17:04 CEST

Con test board ancora sulla release Recorder r4 e senza nuovi interventi, la
telemetria Tracker ha isolato un `read` da2551,925ms alle17:04:32:
`frame_io.transaction.max=2548,016ms`, worker CPU max9,679ms e inferenza
massima34,75ms. Il broker ha continuato a pubblicare30/20/15FPS e il
riepilogo nativo della stessa epoca non registra timeout/corruzioni di
cattura; e` distinto dallo stallo detector delle15:21. La transazione
comprende invio richiesta, risposta del server, payload e SHA client AF_ALG:
il solo wall lungo non identifica ancora quale sottofase abbia atteso.
Anche la sync Recorder da12,333s terminata alle17:03:56 non e` causalmente
attribuibile a questo read successivo. Occorre separare le fasi di successo
slow-path lato FAP/SDK senza log per frame, conservando deadline, integrita`
e backend HW; poi correlare con I/O/termica e una ricorrenza fisica.
Diagnostica candidata source-only: il peer nativo espone sullo stesso thread
quattro tempi bounded (send, risposta descrittore, ricezione payload,
integrita` SHA) letti dal SDK Python e aggregati dal Tracker nel riepilogo
gia` esistente. Nessun frame, log sincrono per frame, deadline o fallback
cambiato. Test SDK45PASS e Tracker611PASS su host con socket Unix corto;
serve ancora build ARM, promozione misurata e prova fisica. Il primo run
Tracker con basetemp pytest troppo lungo falliva un fixture di socket Unix
(`sun_path`108byte), non il runtime FAP; ripetuto con basetemp corto passa.

### Strumentazione FAP installata su test board — 30 settembre

La candidata ARM per le quattro sottophasi native FAP ha superato build e
smoke ABI. R1 non ha commutato la root per helper `operator_sync` assente dal
PAL; r2 ha commutato ma postflight FAILED perché il payload Python era mode600
e Tracker/Registry non potevano importarlo. Una validazione generica dei
permessi, con due regressioni e19test setup PASS, blocca ora tale pacchetto
prima della copia. Non sono fault dell'H.265 o dello SHA. PAL r1/r2 conservati.

R3 ha promosso lo stesso strumento con modi644/755 corretti: manifest
`c9f4f446fbfc72d1c7c2ce627d417071452c3ddaa5d4ff28b16954abd9aac003`,
root `dev-1789244163760113403-c9f4f446fbfc72d1-ab9f3f39d397`,
postflight PASS18:39:45 CEST. Misura indipendente9402entry/554425194byte,
15hash payload/quattro config PASS; supervisore NRestarts0, broker e Tracker
in avanzamento. Nel primo campione di30s la transazione FAP aveva
media15,144ms/max25,918ms; invio0,105ms, risposta descrittore4,659ms,
payload5,933ms, SHA3,527ms medie. Non si è ripetuto il wait2,5s, quindi
il collo originario non è ancora localizzato né corretto. La sorgente è
configurata30FPS ma ha fornito circa16–19FPS effettivi nel primo periodo
dopo l'attivazione, senza timeout/corruzione; non qualifica il30FPS diurno.
Nessun nuovo soak, nessun intervento su deployed board spento.

Revisione statica del percorso server dopo r3: `FrameSession.Step` distingue
gia` `receive`, `authorize` e `send_delivery`; `DeliveryPublication.Snapshot`
separa authority/resource/provider/header/validate/consumer e
`FrameSender.SendDelivery` registra i checkpoint di snapshot e scrittura.
Tuttavia `delivery_diagnostics.go` rende questi dettagli **solo in caso di
errore**. Uno stallo che termina con successo puo` apparire al client come
`native_descriptor` lungo senza un sottostadio server nel log. Non aggiungere
un logger per frame alla cieca: alla prossima ricorrenza usare prima i quattro
tempi r3. Se domina il descrittore, estendere in modo bounded la diagnostica
server del solo slow-path di successo, distinguendo snapshot/provider da
trasporto/scheduler; se domina integrita` o payload lato client, intervenire
in quel tratto. Questo e` un limite di osservabilita`, non ancora una causa.
