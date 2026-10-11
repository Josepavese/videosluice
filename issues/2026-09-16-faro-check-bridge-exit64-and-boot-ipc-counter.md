# Check test board 16 settembre: bridge exit64 ricorrente e contatore IPC di boot

## Chiusura serale 21 settembre: escalation del supervisor distribuita, cadenza camera, baseline SD

Serata del 21/09, dopo la riparazione del drift di contratto (dettagli e
interventi di codice in `improvements/2026-09-21-test-board-contract-drift-and-supervisor-robustness.md`).

**Escalation del supervisor ora su test board.** Seconda promozione (`main.go` +
restart) completata: switch alle 19:49 sulla release
`dev-1789244163760113403-5a97e2035445cfee-7135445a6219`, misura `ok:true` (9362
entry, 553.981.359 byte), servizio `active` dalle 19:49:48. Snapshot dopo il
riavvio: `ipc_failures=0`, tutti e 14 i task `running=true` con `starts=1` e
`exits=0`. La promozione da 500 MB ha richiesto **34 minuti** (19:15 → 19:49)
con il processo in `D` su `folio_wait_bit_common`: ~250 KB/s effettivi.

**`main_fps=12.95` non e` un guasto del dispositivo.** Metriche native del
bridge: su un intervallo di 77 ms il tempo di lavoro e` 10,65 ms e l'attesa
sulla camera 66,5 ms (`wait_avg_ms`), con `capture_timeouts=0`,
`capture_corrupt_frames=0`, `main_drops=0`, `main_max_queue=0`. Il limite e` la
consegna dei frame da parte della webcam (budget di elaborazione usato al 14%).
La camera e` in `auto_exposure` apertura prioritaria con `gain` 4/16 ed espone
un controllo "Dynamic Framerate": con poca luce allunga l'esposizione e
consegna meno frame. La giornata lo conferma: 30 fps pieni dalle 11:00 alle
16:40, ~13 fps dalle 19:10 (crepuscolo). Non e` termico (`cooling_device` 0/4 e
0/3, CPU a 1,5 GHz che e` il massimo, trip 95/110 °C) ne` di disco.
Da riconfermare domani a luce piena: se resta a 13 fps, il sospetto si sposta
sulla camera o sul negoziato UVC.

**La latenza di `sync` e` di fondo, non delle promozioni.**
`native_archive_checkpoint_slow` compare **20 volte tra le 11:10 e le 16:30** in
esercizio ordinario, senza alcuna promozione in corso (circa uno ogni 10
minuti), oltre ai tre caduti nella finestra di scrittura della promozione. La
scheda e` quindi cronicamente lenta sul `fsync`: nessun tuning software lo
rimedia, resta il candidato numero uno alla sostituzione.

## Aggiornamento 21 settembre: freeze del 19/09, exit64 e` un'uscita pulita, flap del frame-source

Stato al 21/09 dopo accensione manuale (09:47 locali): release attiva
`dev-1789244163760113403-d0a3942873fa7aae-f04a508b0349`, misura `ok:true`
9360 entry / 553725857 byte, invariata prima e dopo tutte le azioni seguenti.
Nessuna scrittura sotto `/opt/nemesi` in tutta la sessione.

**Freeze del 19/09 ricostruito dai log applicativi.** Il journal non ha
registrato nulla (sotto), ma i log applicativi coprono l'evento: ultimo record
19/09 07:44:34 CEST, ultime scritture 07:45:09 (spool person-registry), ultimo
segmento video completo `20260919-053000.mp4`. Il log applicativo termina con
1723 byte NUL, cioe` un flush rimasto incompleto. Nessun errore applicativo:
tutti i record sono `info`, recorder `archive_state=writing` e
`retention_state=healthy`, person-registry `state=running`. Il dispositivo
**non si e` riavviato**: il segmento interrotto `20260919-054000` e` stato
marcato `stale unpublished archive observed after restart` solo al boot del
21/09 (09:49:14), quindi e` rimasto acceso e bloccato fino allo spegnimento
manuale (~2 giorni). Ultimo contatto Tailscale 05:57:58Z, ~13 minuti dopo
l'ultima scrittura applicativa. `/sys/fs/pstore` e` vuoto: nessun messaggio di
kernel sopravvissuto. Sul dispositivo `hung_task_timeout_secs=120`,
`hung_task_panic=1`, `panic=5`, `panic_on_oops=1`, `softlockup_panic=1` e
`console=ttyS0,115200`; il watchdog hardware non e` funzionante
(`dw_wdt 34250000.watchdog: No valid TOPs array specified`), quindi un blocco
non ha recupero automatico.

**Journal cieco di nuovo, ma non riproducibile.** Fra il 18/09 16:41:18 e il
21/09 09:47:30 non esiste una sola entry e nessun file di journal ha mtime in
quella finestra. Nei boot del 16 e 18 settembre journald si era fermato 72 e
57 minuti dopo l'avvio, con il dispositivo vivo; il 21/09 **non si e` fermato**:
136 campioni fra 11:04 e 12:15 (122 in `epoll_wait`, 14 in
`do_get_write_access`/`folio_wait_bit_common`/`wait_transaction_locked`,
ritardo di scrittura massimo 10s, 18 campioni con ritardo >=3s). La regola
"si ferma un'ora dopo il boot" non regge; il meccanismo resta ignoto.

**Latenza storage misurata dal software.** Con pipeline attiva il recorder
registra `native_archive_checkpoint_slow` con `sync_ms` 3520, 4037, 9060 e
13970 (soglia severa definita a 3000 ms); una lettura del journal ha impiegato
~60s (11:51). Sulla macchina c'e` `/swapfile_nemesi` da 2 GiB **sul rootfs SD**
con ~1 GiB in uso (nessun OOM registrato, nessuna attivita` di swap nei
campioni vmstat): rischio latente, non causa dimostrata. Disco `/` all'84%,
15 GB di registrazioni, `MemTotal` 3,1 GiB.

**`exit_status=64` non e` il guasto: il worker esce dopo un rilascio
confermato.**

    [capture-lifecycle] event=uvc_capture_closed signal=0 fd=8
      streamoff_attempted=1 streamoff_rc=0 buffers=4 unmapped=4 close_rc=0
      release=confirmed streamoff_outcome=stopped
    [worker-release] operation_status=1 sdk_release=confirmed exit_status=64

Il guasto e` a monte, sullo stream `analysis` (consumatore tracker):

    [frame-source] event=source_output_slow source=front source_epoch=15 stream=analysis
    [frame-source] event=frame_source_failure stage=output_send code=-5 peer_stage=io_deadline source=front
    [rdkx5_v4l2_mux_bridge][frame-source] stage=output_send failure_code=-5 published=0 send_stage=5 bytes=343140

la sorgente frame non consegna entro la scadenza I/O, l'epoca viene chiusa e il
tracker perde il peer (`PeerError ... code=-6`) ricadendo nel loop
`source_pending` ogni 2s. Secondo guasto indipendente:
`[encoder] event=encode_failed stage=dequeue_output sdk_code=-268435443
codec=2 640x360`. Churn misurato: epoche 4->23 in nove minuti (11:13:45 ->
11:22:35), un flap ogni 30-60s, in coincidenza con i sync da 4-14s.
Ipotesi (non provata): il consumatore analysis diventa lento perche` il suo
lavoro per-frame passa dalla SD satura; il bridge sfora la deadline e abbatte
l'epoca.

**Azioni eseguite (autorizzate).** Aggiornamenti automatici disattivati su
test board: `apt-daily.timer` e `apt-daily-upgrade.timer` `masked`,
`/etc/apt/apt.conf.d/20auto-upgrades` a `"0"` (verificato con `apt-config
dump`); nessun upgrade era in corso (timer 21/09 10:41:06 -> "No packages
found"). Restart del controller alle 11:01 (`restart_controller_remote`):
pipeline recuperata (main 30,07 / analysis 20,06 / archive 14,97 fps, tracker
18,16 fps p50, `ipc_failures=0`, archivio misto ripartito al confine di
segmento), poi nuovo degrado alle 11:52 per il flap.

**Watchdog: OK non significa funzionante.** Alle 11:40 il watchdog riportava
OK con `tracker_exits=22` (flap in corso); alle 12:10 corretto in ALARM con
`tracker_running=false`. I check attuali (booleani + contatore IPC) non
distinguono il flap. Da estendere con: churn epoche negli ultimi 15 min,
`sync_ms` massimo del recorder, delta delle uscite del tracker.

**Prossimi passi proposti (non eseguiti).** Cattura seriale su `ttyS0` a
115200 per il prossimo freeze; zram al posto dello swap su SD; watchdog
esteso; passata di lettura completa della SD sotto osservazione MMC; nessun
soak finche` il blocco non e` spiegato.

## Aggiornamento 18 settembre: stallo del dispositivo, journal cieco, restart risolutivo

Stato al 18/09: release attiva `dev-...-6f9252378d8f9eb3-894d7e1a7227` (roi-rounding,
attivata il 16 sera), misura `ok:true` 9360 entry / 553725857 byte, invariata
prima e dopo le azioni seguenti. Nessun file della release modificato a mano.

**Stallo e spegnimento brusco.** Ultimo segmento video completo
`20260918-073000.mp4` (chiuso 07:40 UTC), il successivo incompleto; ultimo
contatto Tailscale 07:45:40Z. Il dispositivo si e' fermato il **18/09 verso le
07:40-07:50 UTC** (~09:45 locali), non il 16 come suggeriva la sequenza dei boot
nel journal. Al riavvio il kernel ha recuperato il filesystem (`3 orphan inodes
deleted`, `recovery complete`, FAT "not properly unmounted"): spegnimento
brusco, non shutdown pulito.

**Il journal non ha registrato lo stallo.** Il journal di sistema si ferma al
16/09 14:51 e quello utente alle 22:33; tra il 16/09 sera e oggi non esiste una
sola entry (`journalctl --since/--until` su tutto il journal). I file sono
integri (`journalctl --verify`: PASS) e coerenti con il riavvio controllato del
16 sera, quindi non e' corruzione ma **assenza di scrittura per circa due
giorni**, mentre la pipeline continuava a girare. Questo collega il finding agli
stalli I/O su journal/MMC gia' osservati il 16/09 10:26-10:27 UTC; il segmento
`.failed/20260917-082000` mostra che il degrado era gia' in corso il 17. La
causa ultima del freeze resta non determinabile **proprio per assenza di log**:
ripristinare la scrittura del journal e' il prerequisito per diagnosticare
eventuali ricadute.

**Post-riavvio: pipeline bloccata, poi recovery col restart del controller.**
Dopo lo spegnimento/riaccensione il video-broker restava in
`capture_recovery_start_failed ... frame source cleanup unconfirmed: context
deadline exceeded` ogni 30s, con il tracker in crash-loop ogni 2s (`bootstrap
stage=admission failed: frame startup stage=source_pending`) e
`ipc_validation_failures=1` di nuovo per il salto di clock all'avvio. Il restart
del controller (percorso canonico `restart_controller_remote`, 16:00 locali) ha
risolto: camera ammessa (`status=admitted`, vps+sps+pps, 29,41 fps osservati),
broker `health_state=running` con main 30,00 / archive 15,00 / sub 20,00 fps e
zero drop, tracker `status=ok` a ~18 FPS senza ulteriori uscite,
`ipc_validation_failures=0`. Il contatore IPC del Finding 2 e' quindi ora
azzerato dal nuovo avvio del supervisor; NRestarts del servizio resta 0.

**Nuovo ostacolo per la qualifica.** Sulla release attiva
`setup/tools/recovery_soak.py` e' a mode **644** invece di 755 (mtime 16/09
21:44, uid/gid 0, nlink 1), quindi il gate canonico rifiuta il soak con
"active recovery soak helper metadata is unsafe" (richiesto 0755 esatto,
`recovery_soak.py:1107-1110`). Va corretto con il percorso di promozione, non a
mano sulla release misurata. Fino ad allora nessun soak canonico e' avviabile.

**Esito delle azioni autorizzate (18 settembre, sera).** Permessi corretti con
il percorso canonico: contenuto del tool identico al sorgente (sha256
`97524bd5a041b45dd59348e3...` su entrambi), differiva solo il bit esecutivo;
`./ssh/sync.sh setup/tools/recovery_soak.py` ha promosso la release
`dev-...-d0a3942873fa7aae-f04a508b0349` (manifest
`d0a3942873fa7aaed7cda6320c863b907c3a5ee0eee34e9ad48355e64504af42`,
`build_performed=false`), con file a 755 e misura `ok:true` invariata
(9360 entry). Verificato end-to-end: `load_runtime_policy` e un campione
autenticato `collect_sample` completano — **il gate soak e' di nuovo aperto**
(`ipc_validation_failures=0`, tracker running). Nota operativa: lo switch del
link senza restart lascia i processi sulla release precedente e il tracker
ricade in `source_pending`; il restart del controller allinea tutto (fase che
`sync.sh ... restart` esegue). Eseguito anche `fsck.fat -a` sulla partizione
CONFIG (`/dev/mmcblk1p1`, montata su `/boot/config`, nessuna dipendenza e
contenuto inerte): solo il dirty bit era settato, ora rimosso e partizione
rimontata. Aggiunto `ssh/watchdog.sh` (read-only: tailnet, journal,
nemesi-agent, snapshot supervisor) con automazione ogni 30 minuti.

## Lavorazione autorizzata 16 settembre

L'utente ha richiesto di risolvere questi finding e il blocco Registry del giorno
precedente. Nessuna chiusura dal solo stato `ready` o dal recupero del bridge.

- **ROI:** il 15 settembre ultima osservazione DB alle 10:06:22.556 CEST;
  canale ROI ritirato alle 10:06:26.781 (`reply_write`), poi
  `provider_unavailable` persistente, mentre il Tracker continuava. Il SDK
  chiudeva il client ROI ma il reader delivery non ne verificava la chiusura;
  il cropper trasformava l'eccezione in crop assente. Correttiva locale:
  retirement della coppia di canali, nuovo binding attraverso il recovery già
  esistente e primo errore conservato in log bounded. Le risposte negative
  complete, come `expired`, non causano restart. Test RED riprodotto e GREEN;
  integrazione con CausalNV12FrameSource testata, non ancora prova onboard.
  SQL read-only successivo conferma il primo record dopo il blocco alle
  17:43:57.261 CEST del 15 settembre: ripresa dopo il recovery del bridge/Tracker,
  con oltre 7 ore senza osservazioni. Il 16 settembre 12:13 CEST il vecchio
  runtime sta producendo nuove osservazioni; non attribuire questa ripresa
  alla correzione ancora in compilazione.
- **Clock:** età del frame causale basata sulla ricezione monotona, timestamp
  wall originale preservato. Evidenza con età non rappresentabile invalidata,
  senza clamp di freschezza o modifica dello schema. Test salti avanti/indietro
  e invecchiamento reale; distribuzione ancora da verificare.
- **Bridge:** journal 16 settembre 07:34:30 UTC identifica la causa immediata
  dell'epoch 4: `interframe_capture_discontinuity`, sequence 983457->983650,
  delta 193 dopo frame_work massimo 6472.57 ms. Non una chiusura spontanea
  provocata dal SIGTERM: SIGTERM appare durante il recovery successivo.
  Le fasi misurate non spiegano quei 6.5 s (write_sub max 13.64 ms,
  decode max 2.21 ms, archive_encode max 4.79 ms). Causa ultima ancora aperta;
  aggiunta diagnostica bounded delle fasi mancanti e conservazione del motivo
  capture-integrity nel riepilogo, senza allentare le guardie H26x.
- **Health:** il riepilogo leggeva lo stato cached, aggiornato solo da un
  consumatore status; correzione locale lo ricalcola alla cadenza del log.
- **Retention:** verificata attiva, limite 16106127360 byte; run 341/344/346
  del 16 settembre eliminano normalmente gruppi scaduti e rientrano nel limite.
  Nessuna cancellazione manuale o aumento quota necessario. Swap occupata da
  sola non prova pressione attiva; PSI non esposto da questo kernel.

Prove private persistenti: `test-board-registry-20260915.1v3SD3r6` e
`test-board-reliability-20260916.CnLlkL5v` sotto il PAL workstation
`<private-workspace>`. Preservare gli esiti sotto: la diagnosi
iniziale e le qualificazioni precedenti non vengono cancellate dalla correzione.

### Tentativo di attivazione 16 settembre, 10:20-10:22 UTC

Build e test della correzione completati su test board, senza modificare il runtime.
L'attivazione misurata è stata **rifiutata prima dello stop**, non distribuita:
guardia quiesce exit 75, 240 campioni in 94,199 s, nessuna sequenza di tre
campioni puliti. Receipt `promotion/failure.json`: `stopped=false`,
`activated=false`; supervisor 3217/invocation invariata.

Thread osservati in D: bridge 343318/tid343325 in `hash_sendmsg`; nell'ultimo
campione anche Python 343351/tid343593 in `jbd2_log_wait_commit`.
Questi sono campioni di attesa, **non prova che un singolo hash sia rimasto
bloccato per tutti i 94 secondi**. Il percorso AF_ALG può attendere il driver
oltre i timeout socket; non attribuire però tutti gli incidenti alla SHA o alla
SD senza misurare avanzamento e I/O. Nessuna modifica a provider o timeout.
Letture SSH successive intermittenti, con timeout TCP/22 e risposte ICMP
ritardate; ping Tailscale diretto riuscito (63 ms). Rete e attese del device
restano da distinguere. Nessuno stop forzato, reboot o modifica a deployed board.

Job persistente sul device:
`/var/lib/nemesi/state/operator-backups/reliability-20260916.vNfovhJz`.
Conservare il tentativo fallito e riusare i binari solo dopo verifiche di
identità/misura e una finestra sicura; non ripetere build/deploy alla cieca.

Alle 10:26-10:27 UTC, misure `/proc` mostrano I/O wait 51-68%, 9-17 task
bloccati, journal/MMC, Tailscale e diversi componenti Nemesi in attesa di pagine
o commit. Nessuno swap-in/out negli intervalli vmstat; circa 1,7 GB disponibili.
APT automatico, iniziato alle 10:21:18 UTC, risulta terminato alle 10:30;
I/O wait nei tre campioni successivi 5,67/14,18/21,17%. Non una diagnosi di
SD difettosa né dimostrazione che APT sia l'unica causa degli stalli storici.
Il sorgente del recorder installato coincide con quello canonico, inclusa la
correzione del backoff checkpoint; nessun hotfix recorder perso.

Secondo tentativo: quiesce e arresto regolare passati; promozione misurata
avviata alle 10:33:25 UTC, manifest
`9c9631bdf035090a599d40b988ca478c071e9dfa00a325369b7b90b790fb346a`.
Ricevute separate in `promotion-r2`, senza cancellare il primo tentativo.
Postflight e soak ancora da completare; non equivale a incidente risolto.

Test aggiuntivo integrato locale PASS: canale ROI nativo Linux chiuso dal
broker-fixture dopo due crop verificati, con processo broker e canale delivery
indipendente ancora validi. Lo SDK ritira il client e il Tracker solleva
`FrameSourceRebindRequired` per `provider_unavailable` con causa `PeerError`,
invece di proseguire senza ROI o attendere un guasto del canale video.
Test `TestNativeDecoderSurfaceToExactROI` con tag `nemesi_integration`, 13,659 s;
copre trasporto reale e SDK/cropper/emitter/validatore Registry, con decoder e
ammissione hardware fixture. Non una prova ottica della camera fisica.

### Attivazione riuscita e qualifica in corso

Promozione e postflight PASS. Root misurata:
`dev-1789244163760113403-9c9631bdf035090a-3a65fb1da845`; supervisor 350285,
invocation `3dc76f318416419b93a4e6e5892f5414`, NRestarts 0, quattro configurazioni
invariate. Soak tecnico development da 2700 s iniziato alle 10:50:05 UTC;
non una nuova attestazione ufficiale firmata, né ancora un soak completato.

Alle 10:51 UTC: broker `health_state=running`; pipeline circa 30/20/15 FPS,
resize analysis hardware, nessun errore di pubblicazione nel primo minuto;
Tracker circa 18,6 FPS, letture/mapping senza errori. Registry 46 osservazioni
elaborate dopo il riavvio, failed 0/coda 0: prova di avanzamento globale, non
di identificazione facciale di Salvatore. Campioni autenticati iniziali del
soak PASS, quindi contatore IPC valido sul nuovo avvio (non azzerato ad hoc).
Temperatura osservata 77-79 °C. La causa ultima delle pause storiche resta
aperta; nessuna deduzione di assenza leak dalla sola ripresa.

La prova del fault ROI reale passa anche con Go race detector (14,979 s).

### Primo soak fallito: separare la diagnostica dal campionamento

Il primo soak NON è passato: 414 campioni, gap completamento 5257 ms,
raccolta 4868 ms; fase autenticata più lunga riportata `identity_before`
25 ms. Il campione coincide con la pubblicazione del report ogni 30 s:
il wrapper scriveva/flusciava sia JSONL sia stdout sulla SD dentro il callback
cronometrato. L'attesa di diagnostica è l'ipotesi principale, non una certezza
deducibile dal solo massimo di fase. Nello stesso intervallo la pipeline
mantiene 30/20/15 FPS, gap capture circa 44 ms; nessun restart o errore lettura.

Correzione del solo observer operativo, nessun nuovo deploy: progress journal
bounded su tmpfs verificato, niente stdout/SD nel callback; archivio su PAL
persistente al termine o su errore. Aggiunta fase `observer_progress` esplicita
per non attribuire a systemd il tempo non spiegato dalla diagnostica.
Collector autenticato installato, campionamento 0,5 s, limite gap 5 s, durata
2700 s, FPS e termici **invariati**. Nuovo tentativo da zero con ricevute
`soak-r2`; l'esito fallito `soak/` resta conservato e non viene riclassificato.

## Snapshot diagnostico iniziale (prima della lavorazione sopra)

- Stato: aperta; evidenza diagnostica read-only raccolta durante il punto di
  arresto del goal, nessuna mutazione eseguita (nessun restart, deploy, soak o
  scrittura sotto `/opt/nemesi`)
- Severita`: media; il recupero automatico funziona (~5-10s di buco video per
  episodio), ma la cadenza di ricorrenza e l'impatto sul gate soak vanno gestiti
- Rilevata: 2026-09-16 ~08:40-09:00 UTC, dispositivo nemesi-test-board-2579
- Componenti: video-broker (bridge V4L2), tracker (rebind by design),
  supervisor (contatore IPC), recorder/archivi (spazio disco)
- Baseline esaminata: promozione development `054b59c03ecdcb2c3c0809b5c82614d553cd2c0b2650b90a827c711ef079bb3f`
  del 13 settembre, invariata
- Correlata: [invio FAP interrotto e causa persa](2026-09-09-native-frame-send-failure-loses-cause.md)
  (exit64 riassunto come `child_exit_without_terminal_reason`),
  [deadline delivery snapshot e riavvio Tracker](2026-09-10-delivery-snapshot-deadline-and-tracker-restart.md)
  (`io_deadline` su output_send)

## Come e` stato eseguito il check

Accesso canonico (`ssh/recover_access.sh` + `ssh_cmd`/`ssh_sudo_cmd`, host key
pinnate), solo comandi read-only; le sonde Python sulla release attiva con
`python3 -I -B`. Fonti: `/var/lib/nemesi/setup/{release_state,update_state}.json`,
verifica misura `runtime_measurement.py verify`, snapshot autenticato del
supervisor health socket via `recovery_soak.authenticated_snapshot` con
`load_runtime_policy(development_manifest_sha256=054b59c0...)`, journal
`nemesi-agent` finestra 2026-09-15 06:40 UTC -> 16 settembre, `/sys/class/thermal`,
`ps`/`free`/`df`.

## Stato installato e operativo: conforme al punto di arresto

- `release_state.json` `development_dirty` con manifest `054b59c0...`;
  verifica misura `ok:true` 9358 entry / 553727013 byte, identica al 13 settembre.
- `update_state.json` `failed` e` residuo delle 08:45 UTC del 13 settembre
  (`nemesi-update-check.service`, "failed before downtime; healthy release kept
  the same PID"): coerente con l'intento CI orfano gia` documentato, non e` un
  evento nuovo.
- `nemesi-agent` attivo dal boot del 15 settembre ~06:45 UTC, NRestarts=0;
  tutti i 15 task running/connected nello snapshot (non prova di continuita'
  dei figli dal boot: i restart sono descritti sotto). Pipeline: live 30,0 FPS 1920x1080 h265,
  analysis 20,0, archive 15,0, zero drop; tracker 18,1 FPS p50; Person Registry
  ready, coda 0 (6628 osservazioni, 2 scarti); termico 79,7/80,1 °C (DDR/CPU),
  lontano da soft 90 °C e dal max 84,5 °C del soak.

## Finding 1: cinque terminazioni del bridge in 26h, exit64 a regime

Il bridge V4L2 ha terminato 5 incarnazioni in 26h (l'incarnazione di boot, poi
le epoch 1-4; l'epoch 5 era in corso al check); ogni terminazione ritira la
delivery causale e il tracker esce con `FrameSourceRebindRequired` (exit 1 by
design, `causal_nv12_source.py:190`) per poi riavviarsi con una nuova grant.
I restart del tracker non sono bug del tracker ma il re-binding progettato.
La numerazione grant `source_epoch` del supervisor coincide con la `media_epoch`
del broker; l'incarnazione di boot e` pre-numerata.

Incarnazioni del bridge (incident 3346-N, media_epoch N):

| Incarnazione | Intervallo (UTC) | Durata | Fine |
| --- | --- | ---: | --- |
| boot | 15 set ~06:45 -> 07:00:57 | ~16 min | ritiro delivery, exit tracker 1 (prima ora) |
| epoch 1 | 15 set 07:00:59 -> 07:03:41 | ~3 min | ritiro delivery, exit tracker 2 (prima ora) |
| epoch 2 | 15 set 07:03:43 -> 07:34:44 | ~31 min | ritiro delivery, exit tracker 3 (prima ora) |
| epoch 3 | 15 set 07:34:44 -> 15:43:44 | 8,15 h (dichiarata 29338866 ms) | `capture_epoch_ended outcome=failure reason=frame_source_failure:output_send:io_deadline`, exit64 |
| epoch 4 | 15 set 15:43:48 -> 16 set 07:34:33 | 15,85 h (dichiarata 57045547 ms) | `child_exit_without_terminal_reason`, exit64 (SIGTERM su capture, cleanup SDK confermato) |
| epoch 5 | 16 set 07:34:37 -> | in corso (~1,5 h al check) | — |

I cinque exit del tracker (07:00:57, 07:03:41 e 07:34:44 UTC del 15; 15:43:46
UTC del 15; 07:34:35 UTC del 16, seguito da un tentativo fallito in ammissione
`source_pending` e riuscito alle 07:34:39 con grant epoch 5) sono tutti firmati
`FrameSourceRebindRequired`. Le durate di boot/epoch 1-2 sono ricostruite dai
timestamp journal; quelle delle epoch 3-4 sono dichiarate dal broker. Il recupero automatico funziona ogni volta: `capture_recovery_scheduled`
retry 3s, ammissione ~5,4s, cadenza degradata su tutti e tre gli stream per ~4s
e poi `stream_cadence_recovered`.

I due motivi osservati sono gia` tracciati come issue aperte: la perdita di
causa in `child_exit_without_terminal_reason` e la famiglia `io_deadline` su
output_send. Questo check aggiunge: (a) la ricorrenza su test board e` di ~1 episodio
ogni 8-16h a regime, non solo legata a switch ottici; (b) tre terminazioni
concentrate nella prima ora dopo il boot (incarnazione di boot, epoch 1 e 2),
poi intervalli che si allungano: pattern compatibile con una condizione di
warmup o contesa risorse iniziale meritevole di indagine dedicata; (c)
l'epoch 4 e` terminata con il child che ha chiuso la capture su SIGTERM e cleanup SDK
confermato prima di uscire 64, quindi senza danno hardware dimostrato.

## Finding 2: ipc_validation_failures=1 permanente su questo boot

Alle 06:59:29 UTC del 15 settembre, durante il salto di clock NTP (board senza
RTC: da epoch anno-2000 a 2026), il tracker ha pubblicato un
`tracker.status.updated` con `source_frame_age_ms` oltre il massimo di schema
(86400000 ms): il supervisor ha respinto e conteggiato l'evento. One-shot, non
piu` ripetuto in 26h.

Implicazione operativa: il gate del campione/soak canonico richiede
`ipc_validation_failures == 0`; il contatore non si azzera finche` il
supervisor non riparte, quindi **qualsiasi soak canonico avviato ora su questo
boot fallirebbe al gate**. Considerazioni aggiuntive:

- il fenomeno puo` ricapitare a ogni boot in cui NTP sincronizza dopo la
  prima pubblicazione status del tracker; e` un artefatto di clock-step, non
  una contraffazione di binding;
- il worktree locale ha gia` in corso modifiche al contratto
  `tracker.status.updated.schema.json` (pattern `source_generation` verso
  `fap1:<sha256>`, file `src/tracker/tests/fixtures/causal_tracker_status.py`):
  la valutazione di come immunizzare l'eta` dal clock-step (clamp o rifiuto
  esplicito) va fatta in quel lavoro di contratto, non con un rilassamento
  ad hoc del massimo;
- alla ripresa del lavoro sara` necessario o un riavvio del supervisor
  (mutazione da autorizzare, azzera anche NRestarts) o una decisione esplicita
  sul trattamento di questo one-shot.

## Considerazioni aggiuntive (nessuna azione eseguita)

1. **Disco `/` all'84%** (46G/58G, 9G liberi): l'archive produce ~2,2 Mbps
   (~1 GB/h). Verificare la retention del recorder prima che lo spazio diventi
   il limite; e` il primo vincolo tangibile a medio termine.
2. **Swap ~1 GB su 2 GB in uso** (zswap attivo, memoria disponibile 1,7 GB):
   non bloccante, ma segnale di pressione storica; monitorare nei prossimi
   readback.
3. **Governor BPU**: episodio di pressure stamattina 09:57 locali per ~20s
   (BPU 96%, analysis cap 15 FPS, poi nominal). Comportamento corretto; da non
   confondere con degrado reale della pipeline nei prossimi dati.
4. **`health_state=initializing` persistente**: i `video_pipeline_summary`
   riportano `health_state=initializing elapsed_sec=10.0` anche 1,5h dopo
   l'avvio dell'epoch 5, con pipeline ai target. Probabile etichetta non
   aggiornata dopo il warmup: verifica cosmetica da aggiungere al lavoro di
   diagnostica del broker.
5. **Automazioni**: nessuna attiva nel workspace, coerente con la pausa
   registrata in CURRENT_GOAL.md.

## Prossimi passi proposti (da autorizzare, non eseguiti)

- Aggiungere questa evidenza alle due issue correlate e isolare la causa prima
  dell'exit64 a regime (`io_deadline` output_send e uscita senza motivo
  terminale), inclusa l'ipotesi warmup per la prima ora dopo il boot.
- Decidere il trattamento del contatore IPC di boot prima di ogni nuovo soak
  canonico su questo boot.
- Verificare la retention archive rispetto allo spazio disco residuo.

## Intervento autorizzato e stato al 16 settembre 11:18 UTC

Richiesta utente: risolvere anche le anomalie del 15 settembre. Su test board sono
implementati e installati il recupero del binding ROI terminale, l'eta' del
consumer calcolata su clock monotono, l'aggiornamento dello stato health e
diagnostica bounded delle fasi native precedentemente non coperte.
La causa della pausa nativa storica di 6,5 secondi non e' ancora dimostrata.

Promozione di sviluppo misurata, non release ufficiale firmata:
`9c9631bdf035090a599d40b988ca478c071e9dfa00a325369b7b90b790fb346a`.
Postflight PASS, configurazioni preservate, supervisor avviato alle10:45:45UTC.
SDK42 test, Tracker suite mirate84 test, test Go race e integrazione reale
decoder nativo/SDK/crop/Registry con fault del solo socket ROI PASS.
Osservato nuovo avanzamento Registry senza errori, non prova di riconoscimento
facciale di Salvatore. Fonte front30/analysis20/archive15 osservata invariata.

**Qualifica ancora aperta:** primo soak fallito a414campioni per gap di raccolta
5257ms, coincidente con pubblicazione sincrona dei progressi su SD. Il secondo,
con soli progressi bounded in RAM e identici gate, fallisce a344campioni per
`recorder raw audio persistence writer made no progress`, senza overrun
segnalato. Evento coincidente con finalizzazione del segmento alle11:10UTC;
diagnosi recorder in corso, nessuna tolleranza o soglia ampliata. Non sono
soak positivi. deployed board resta invariato. Nessun reset/cancellazione del corpus.

Evidenze private persistenti test board:
`/var/lib/nemesi/state/operator-backups/reliability-20260916.vNfovhJz/`
(`promotion-r2/`, `soak/`, `soak-r2/`). Sorgenti riconciliate nel checkout;
prove e controller locali in
`<private-workspace>`.
Non chiudere l'issue per il solo deploy.

## Limiti

Check read-only in un singolo momento: non e` un soak, non prova stabilita'
dell'epoch 5 oltre ~1,5h, e le durate dell'incarnazione di boot e delle
epoch 1-2 sono ricostruite dai timestamp journal. Nessuna soglia, durata o
qualita` attenuata per ottenere questa lettura.

## Misura 2026-09-21: la catena causale dell'`io_deadline` e` confermata

Check read-only su test board durante un episodio in corso. Il dispositivo si e`
svegliato alle 09:47 e la capture e` stata in `source_pending` da subito; il
controller e` stato riavviato per la promozione di sviluppo alle 16:22:55
(supervisor `98842`), con recupero reale ma **durato circa dieci minuti**.

Sequenza dell'epoca 9 (`incident_id=98884-9`), la piu` documentata:

1. 16:38:04 `capture_epoch_started media_epoch=9 child_pid=102614`,
   bootstrap `startup_elapsed_ms=2630`, camera ammessa a `observed_fps=30.85`,
   `fourcc=0x43564548` 1920x1080 h265.
2. L'epoca ha girato 59,7 s su 1760 frame a `fps=29.50` (main frame ok).
3. In parallelo il consumatore analysis perdeva terreno: il tracker registra
   `native_roi stage=fetch_or_encode role=body|head reason=expired` con
   `delivery` 1466, 1779, 1921, e `person registry admission recovered;
   busy=46..65 coalesced=35..49`; `TrackingSummary fps_p50=18.14`
   (`target_ratio=0.96`, `human_ms_p50=29.0`).
4. 16:40:09 `event=source_output_slow stream=analysis`, poi
   `event=frame_source_failure stage=output_send code=-5 peer_stage=io_deadline`
   e `event=native_phase_slow phase=analysis_send wall_us=1001500`: **1,0015 s
   contro un budget di 1000 ms**. Sono 15 us di sforamento.
5. Il bridge chiude da solo la capture (`uvc_capture_closed signal=0`,
   `streamoff_attempted=1`) e esce con `exit_status=64`,
   `reason=frame_source_failure:output_send:io_deadline`,
   `duration_ms=127695 admitted=true`.

L'ipotesi del 2026-09-16 ("il consumatore analysis diventa lento, il bridge
sfora la deadline e abbatte l'epoca") e` quindi **confermata con i numeri**, non
piu` solo plausibile: il consumatore lento e` il ramo tracker->Registry, che
espira le delivery e coalesce le admission, e la conseguenza e` la chiusura
dell'epoca da parte del bridge stesso.

**Il blocco del recupero e` un secondo fatto, distinto.** Dopo la fine
dell'epoca il broker non riesce ad aprirne una nuova:
`capture_recovery_start_failed after_media_epoch=9 err=frame source cleanup
unconfirmed: resource lifecycle not confirmed: stage=release_failed: native
execution stage=sdk_release_unconfirmed process_state=signal: terminated:
peer authentication unavailable`, un tentativo ogni 30 s, **410 nella
giornata**. Da notare l'asimmetria: il bridge dichiara
`[worker-release] operation_status=-1 sdk_release=confirmed exit_status=64`,
quindi il rilascio SDK risulta confermato dal figlio, mentre il broker lo legge
come non confermato perche` il **peer dell'esecuzione nativa**
(`framepeer/port.go:15`, `ErrUnavailable`) non e` piu` raggiungibile
(`frame_execution_linux.go:118`). E` un lead concreto da esaminare: la conferma
di rilascio che il broker pretende puo` essere cercata su un peer diverso da
quello che la produce.

Conseguenza operativa: finche` questa condizione persiste **non esiste un
soak valido**. Il riavvio del controller ricrea il peer, l'epoca riparte e
ricade nella stessa sequenza entro ~2 minuti, quindi un soak fallirebbe
all'ammissione o misurerebbe una pipeline che non acquisisce.

**Esclusioni misurate nello stesso momento** (nessuna supporta l'ipotesi di un
guasto di piattaforma, in particolare della SD): 0 errori MMC/ext4/I/O nel
journal del boot; 0 kill OOM reali; 0 eventi USB di disconnessione con la
telecamera `2bc5:0529 Orbbec` enumerata; scheda SD negoziata a 200 MHz;
`writeback=0`, `Dirty=908 kB`; iowait 0-16% su 5 campioni vmstat con CPU idle
65-93%; swap 1 GB ma freddo (`si`/`so` = 0, `swpd` invariato); termiche 64 °C
(DDR/CPU). La `passata di lettura completa della SD sotto osservazione MMC`
proposta il 16 settembre **resta non eseguita** e la sospensione di I/O storica
(journal cieco per due giorni, iowait 48% il 10/09) resta non spiegata, ma non
e` il fattore di questo episodio.
