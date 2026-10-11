# Deadline nello snapshot delivery e riavvio Tracker

Stato: **APERTO / IN LAVORAZIONE**, bloccante N2/N5/N6 del goal FAP/Registry.
Non assimilare agli stalli host storici B15 ne' a un guasto Tailscale.

## Integrità archivio e cause dei buffer — 5 ottobre14:28 CEST

MP4 nuovo chiuso `20261005-121000.mp4`, scelto come il più recente disponibile:
65.503.453byte, stat sorgente invariata prima/dopo e SHA256 sorgente/copia
`f5e9fddac6bf84893c6f6235c8c6046aa2f15609a2d7ef90b4d8a4f7d3083ae5` uguale.
Trasferimento read-only nice19/ionice idle in RAM host; decode offline a un
thread, non sulla CPU test board. ffprobe e ffmpeg **PASS**, stderr vuoto:600,118s,
HEVC640x360,9000frame e audio Opus; zero frame duplicati/drop del decoder.
L'MP4 del soak1ottobre non è più presente: il decode allora interrotto resta
inconcludente; questo è un PASS indipendente del nuovo archivio sulla stessa
root development, non un nuovo soak o qualifica SD/release/VOD browser.

Campione autenticato successivo20,194s:30,01/20,01/15,00FPS,
Tracker19,16FPS, endpoint termico massimo84,912°C, stesso runtime e contatori
starts4/exits3; Recorder recording senza drop/code/failure retention e
Registry running senza integrity error/stall/fail-stop dichiarati.
Nessun deploy, restart, cambio profilo o collector aggiuntivo. Prove private:
`<private-workspace>`.

Nel sorgente rimaneva la medesima perdita di causalità di `Resource.Borrow`
anche in `Buffer.Borrow`, durante retirement o fencing del processo. Due
regressioni **RED->GREEN** provano il difetto senza SD; ora conserva entrambe
le cause con il helper già esistente, rifiuta ancora il risultato tardivo e
mantiene la memoria addebitata fino al ritorno della callback. Nessun costo
aggiunto sul percorso riuscito o variazione di quota/deadline/logging.
Suite completa broker PASS, framedomain/frameingress race count3 PASS e
vet mirato PASS. Primo race offline bloccato dalla dipendenza dichiarata
x/sys non in cache; prova successiva con checksum verificato in RAM PASS.
Prove `fap-buffer-20261005.ep15sm3o/` nello stesso workspace privato.
Correttiva **non installata**, non attribuisce né risolve i due timeout fisici.

## Readback dopo osservazioni native reali — 5 ottobre13:47 CEST

Campione20,199s, socket supervisor/broker autenticati con peer PID/UID e
invocation: stream live30,00/analysis20,00/archive15,00FPS; Tracker17,18FPS,
contro20,03 nel campione precedente. CPU busy32,89% sul totale della macchina,
iowait17,97%; mmcblk1 media operazioni read141,81ms/write544,08ms (aggregati
che includono attese, non il tempo puro di una fsync). Zero swap-in/out nel
campione,149major fault; non prova assenza di page/file wait. Massimo84,375°C.
Tracker60,60% di un core, nativo41,39%, broker35,89%, Registry26,44%; il
Captioning38,17% e4,75MB scritti, Recorder8,39MB e4,01% di un core.
Queste percentuali per-processo non sono percentuali sul totale della CPU.

Recorder recording/retention healthy, queue/drop0; Registry running,
worker_stalled/fail_stop false, queue/integrity_errors0. Tracker starts4/exits3
invariati, read/mapping errors0: nessun nuovo restart/deadline dimostrato in
questa finestra. L'etichetta IPC connected=false del broker standalone non
sostituisce la verifica del suo status socket autenticato.

La prova ROI separata mostra3nuovi ingressi e relativi asset coerenti, ma
non qualifica20FPS Tracker stabili; iowait e latenza SD sono correlati nel
campione, non causalita' della riduzione FPS o dei timeout storici. La sonda
era read-only/bounded, senza scansione completa del DB, fault, cambio profilo,
secondo collector, restart o installazione diagnostica. Preservare il soak
valido nella sua finestra; non chiamare questo nuovo campione un soak PASS.
Prova privata `roi-outbox-20261005.PornQx/runtime-post.json` sotto
`<private-workspace>`; prossima diagnosi: correlare sottofasi
di Tracker/FAP e attese I/O, senza stressare la SD a fine vita segnalata.

## Ricognizione e correttiva diagnostica — 5 ottobre 2026

deployed board inattivo è fuori dal goal corrente (decisione utente5ottobre, B16).
Su test board resta attiva la root ROI del1ottobre
`dev-1789244163760113403-231560e66de066d7-48cd1f11bfbc`;
non è stata promossa una nuova versione in questa indagine.
Campione passivo20,122s: live30,02/analysis19,98/archive15,01/Tracker20,03FPS,
massimo80,955°C, code Recorder vuote e zero drop. MP4 chiusi fino alle12:30
verificati solo per metadata del filesystem, non per decode completo.
Il precedente soak diurno2700s PASS resta prova distinta, non una garanzia
di assenza di ricorrenze fuori dalla sua finestra.

Tre uscite Tracker dall'avvio2ottobre: una per `source_generation_retired`
alle13:44; due per deadline della delivery locale, poi recuperate:

- 2ottobre16:24:50 CEST: `payload/authorized_write`,3,758492s contro
  budget2,999862s,3record confermati/32680byte payload. Non è copia/SHA.
- 4ottobre07:10:04 CEST: `workspace/borrow`,5,686834s contro
  budget2,999950s,zero record inviati, snapshot non iniziato. Il log precedente
  non separa begin/authority, admission/ledger e preparazione della callback.

Verifica5ottobre13:06: quota CPU e MemoryHigh/MemoryMax non impostate sul
servizio; memory.events high/max/oom/oom_kill tutti0. Non prova l'assenza di
swap/page wait, stallo globale o pressione storica. Latenze di fsync Recorder
fino8,859s; SD a fine vita secondo l'utente. Nessun errore kernel I/O nei
log consultati, nessuna correlazione causale dimostrata tra SD e deadline FAP.
Nessun fault, cambio profilo/soglia, restart, stress SD o secondo collector.

Gap software riprodotto **senza storage**: Resource.Borrow perdeva l'errore
della callback quando il check finale rilevava retirement o cambio processo.
Corretto preservando entrambe le cause (`errors.Is/As`) e il rifiuto del
risultato. Checkpoint bounded error-only separano begin, admission, consumer,
current e finish; nessun log per frame, nuova coda o deadline estesa.
Begin resta aggregato authority/ownership: non lo chiamare tempo di poll puro.
Questa correzione diagnostica **non risolve né attribuisce i due timeout**.

Prove del candidato, **non installato**:3regressioni RED->GREEN,2casi aggiuntivi
begin/admission; suite video-broker completa PASS, race su
framedomain/frameingress/provider Linux count3 PASS. Le prove complete iniziali
con GOTMPDIR lungo fallivano per il limite pathname Unix; l'esecuzione con
GOTMPDIR=/tmp passa senza modifica dei test. Benchmark host Borrow: baseline
1191–1304ns/op, candidato1226–1309ns/op, entrambe840B/op e12alloc/op:
nessuna nuova allocazione, nessun miglioramento pipeline dimostrato.
Test ARM delle nuove regressioni PASS in RAM su test board, nice15/timeout10s,
binario SHA256 `92c148c9ba659379d963462fcbf65e1f72afa61166650869838a26b8fb9977f4`;
MainPID3254/InvocationID9a53efe00959431c9cf80e200410c320/NRestarts0 invariati,
massimo84,424°C dopo il test. Test isolato, non hotfix installato, prova ROI
reale, soak né rilascio firmato. Scratch ARM rimosso nella stessa sessione.
Prove host conservate private in
`<private-workspace>`;
codice e regressioni nel checkout canonico, nessun lavoro unico solo in RAM.

Residuo: attribuire la ricorrenza fisica alle sottofasi e correggere la causa,
qualificare crop/correlazione reali e consolidare una sola consegna pertinente.
Non promuovere soltanto per aggiornare documenti, non forzare un nuovo fault
sulla SD fragile, non chiamare PASS un recovery automatico.

## deployed board14settembre: nuovo riscontro dopo il sotto-rilascio

Il [readback prestazioni](../../../improvements/history/2026-09-14-deployed-board-performance-readback.md)
conferma20FPS attuali, ma task6 starts2/exits1 e media_epoch2 sul candidato
054b59c0. Alle00:01 locali del14settembre, dopo il soak concluso, transazione
delivery epoch1 senza record ricevuti8,871s contro budget1s, threadCPU775us,
`io_deadline code=-5`; recovery cattura e nuovo Tracker. Questo e' un nuovo
episodio, non un fallimento del campione corrente ne' assenza universale di
stallo. Causa primaria non attribuita; nessun riavvio o deploy manuale.
Log nativo nella stessa finestra: source_output_slow analysis wall1.000.708us,
threadCPU995us, socket998089us, integrity2462us, authority_wait3us,
13record inviati/code-5. La pausa e' nel canale locale, non nel calcolo SHA;
non basta a spiegare la mancata lettura del consumer o a correlare due delivery.
La diagnostica SDK del13settembre e' ora distribuita su entrambi i dispositivi,
come registrato nella milestone; le note storiche sotto conservano lo stato
al momento in cui furono scritte.

## deployed board13settembre16:32UTC: errore archive e pausa delivery, recovery automatico

Consolidato development d4e02a2e, supervisor976595 e broker976684 invariati.
Alle16:32:41UTC il nativo976950 riporta `generated archive stream invalid`
H265 `size=-1`; l'API encoder non esponeva la sottofase o il codice SDK.
Nella stessa finestra Tracker: delivery96761/epoch1 ricevuta in23record,
345600byte, transazione7339us; `header_validation_us=4102642`, totale4110366us
contro budget1000000us, threadCPU13424us, integrity non ancora verificata.
Questi tempi indicano attesa/descheduling possibile, non lavoro CPU di4s,
e non identificano da soli GIL, GPU, SD o encoder come causa iniziale.
Nativo `write_sub_max_ms=2933.11`, zero capture_timeout/corrupt; manca il
record `source_output_slow` corrispondente nella finestra conservata. Non
attribuire l'intero write_sub al sender senza coprire anche il lavoro esterno.
La misura archive_encode esistente non include il ramo encode fallito.

Segue EOF sorgente, retirement epoch1 e chiusura SDK/UVC confermata; nuovo
nativo1003171/epoch2 alle16:32:45. Tracker esce alle16:32:43 e riparte PID1003173
con nuovo binding alle16:32:52. Snapshot autenticato16:34:52: Tracker
running/connected, starts2/exits1; Registry starts1/exits0; IPC validation/auth0.
NRestarts del servizio principale resta0 e NON equivale ad assenza restart figli.
Temperatura Tracker87,4-87,5C prima del fault; non e' uno stop termico a90C.
Kernel nella finestra mostra chiusura/riapertura VPU/JPU, senza ulteriori errori
espliciti osservati. Stato memoria successivo non prova lo stato al guasto.

La sonda di costo del collector canonico si ferma al primo campione per
`supervisor task 6 restarted/exited/not-running`: zero campioni validi,
nessuna misura attendibile di costo a regime. Preparazione policy/misura esatta
15,589s nella medesima finestra generale; causalita' col fault non stabilita.
Non ripetere la scansione integrale o aggirare starts/exits per ottenere un dato.
Nessun deploy/restart manuale o modifica profili/soglie; versione mantenuta.

Correttiva diagnostica locale16:40UTC, NON installata: encoder persistente
pubblica un record solo in errore, con stage dequeue/queue input/output,
codice SDK originale, geometria/epoch/capacita' e timeout configurato; separa
layout input/output e bootstrap_copy. Nessun clock, allocazione o log aggiunto
ai frame riusciti, nessun retry o cambio del ciclo di ownership/timeout.
Regressione H264/H265 su4errori SDK, output fuori capacita' e successo silenzioso:
RED con sorgente precedente, GREEN corretta; suite nativa e ASan/UBSan PASS.
Questo colma una lacuna diagnostica; NON e' ancora la soluzione dello stallo.

Prove private persistenti sotto `consolidated-hotfix.J41MC68G` nel PAL locale:
`deployed-board-native-recovery-20260913T1632Z.log`
SHA2569e49f067488e999acfc17ce4259f1ab028e809915302f16b94e1eab8803029d5;
`deployed-board-collector-cost-20260913T1633Z.jsonl`
SHA2566306b4e971727c676e6fea14ab54674fc034e139dc321802cec4db7c4348fd83;
`encoder-failure-diagnostics-{red,tests,asan}.log`.

## Trial13settembre07:12:48UTC: discontinuita' distinta

Candidato firmato f5dccb2d, supervisor1426720, broker1426799/native1427025.
Dopo34,840s di cattura H2651920x1080@30 il sequence UVC passa1008->1023:
delta15, action=retire_source_before_publication. Prima della chiusura:
main29,52FPS, write_sub_max585,12ms, frame_work_max590,50ms;
audio ALSA registra nello stesso istante gap515ms. Tracker19,99FPS,
temperatura70,2-74,4C, poi PeerError receive_record code=-6 in1,368ms
e uscita per source binding ritirato. Questo NON e' un timeout della delivery
e NON prova la sottofase responsabile della pausa nello write nativo.
Trial rifiutato per `task 6 reported exits=1`; rollback fc908a e postflight15s
PASS. Zero soak di questo candidato. Preservare questa distinzione durante
la correttiva: non alzare deadline, ignorare la discontinuita' H265 o imputare
automaticamente la causa alla camera/SD/temperatura.
Prove private `operator-batch.X4d4fi7A/test-board-trial-origin.log` e
`test-board-update-terminal.log` nel PAL locale nemesi-work.

Lettura del percorso nativo13settembre: la pubblicazione della superficie
nativa usa gia' `source_publisher.c` asincrono, ma l'output analysis chiama
ancora `video_broker_frame_source_nv12_output()` sincrono dentro il ciclo di
cattura. `write_sub` comprende verifica authority, SHA del payload e invio
dei record; `send_record` trattiene il mutex authority durante l'invio.
Quindi585ms non sono una misura del solo encoder o della CPU. Il dato rende
plausibile il trasferimento della pressione del consumer alla cattura UVC,
ma non distingue ancora attesa trasporto, hash/driver o scheduling.
Prima di un'altra promozione test board va rivista questa dipendenza: separare
tempo wall/CPU e sottofasi oppure qualificare una pubblicazione analysis
disaccoppiata, bounded e con ownership/correlazione esatta. Non aumentare
semplicemente buffer/deadline e non considerare il solo publisher nativo
asincrono una prova che tutta la cattura sia gia' isolata dai consumer lenti.
Nessuna correttiva di questo percorso e' inclusa nella promozione deployed board r3.

Confronto13settembre con test board fc908a ancora attiva (PID1428266, stessa
invocation, zero restart):20file non-documentali del broker differiscono
da f5, comprendendo test. `native/frame_source_linux.c`,
`framewire/provider/linux/owned.go` e `framewire/provider/linux/bridge.c`
sono invece identici anche per SHA letti direttamente dal device. Il sender
sincrono e la copia/hash dello snapshot non sono quindi nuove implementazioni
introdotte da f5; questo non dimostra che siano immuni a pause sotto carico.
Evitare di attribuire la ricorrenza alle sole modifiche JPU/OpenCL: il percorso
test board H265 con tone off non esercita le sottofasi MJPG interessate.
Manifest filtrato privato `operator-batch.X4d4fi7A/test-board-clean-measurement-current.json`.

Diagnostica nativa aggiunta13settembre, **testata localmente e compilata/testata
su test board; attivazione ancora da verificare**: `source_output_slow` misura anche gli invii che riescono ma
impiegano almeno100ms. Espone wall/thread CPU, preparazione, hash, fase record,
attesa authority mutex e tempo dentro il send; massimo un record al secondo
per sorgente, contatore degli eventi soppressi. Non stampa pixel, non cambia
deadline/criteri, non aggiunge code, retry o allocazioni per frame.
Regressione con invio riuscito ritardato120ms prova visibilita', attribuzione
al socket e limite dei log; restano i test dei tre errori e no-retry.
Suite nativa resize/media/fanout con ASan/UBSan PASS, log privato
`operator-batch.X4d4fi7A/native-output-diagnostics-tests.log`.
Serve la misura sul device prima di attribuire la pausa a una sottofase.
Questo delta NON appartiene al payload congelato deployed board r4.
Onboard08:52UTC: build bridge67,52s e regressione ASan/UBSan PASS,
massimo84,424C; runtime fc908a invariato dalla compilazione. Job privato
`/var/lib/nemesi/state/operator-backups/test-board-native-diag.taNnfmSa`, binarioSHA
`38aa4570a6e8765a8c5324ea9d77e034b43be155ab42cb61623df26174947ceb`.
Promozione mirata richiesta nella unit `nemesi-test-board-native-diag-promote-taNnfmSa`.
Nessuna soluzione della pausa o qualifica del nuovo runtime dichiarata dal
solo test diagnostico. deployed board r5 ha invece superato125,886s integrati sul proprio
profilo MJPG20, senza questa diagnostica: non trasferire la prova a test board H26530.

## Ricorrenza13settembre06:26UTC durante preparazione update

Runtime test board di sviluppo
`dev-1789244163760113403-5a8e2044d18f5405-bcc6849addcd`, supervisor1403806,
broker1403910/native1404117, source/publication epoch1. La contabilizzazione
asincrona causale e' gia' presente: questa ricorrenza non dimostra una nuova
attesa del precedente lock del runtime broker.

Alle06:26:22UTC (ricezione journal06:26:28), prima dello stop dell'updater:
snapshot1562209us su budget999861us, lastdelivery52959, zero record inviati.
Il diagnostico interno, su una seconda riga della catena `errors.Join`, precisa:
authority9us, resource29us, **provider1561623us**, header125us, validate29us;
consumer non invocato, deadline rifiutata dopo il ritorno del provider.
Il tempo e' quindi nel provider dello snapshot, non nelle prime verifiche
authority/resource; non distingue ancora copia/hash, page fault, scheduling o
altre attese interne. Non attribuirlo automaticamente a CPU, SD o acceleratori.
Una ricerca limitata alla prima riga dell'errore perderebbe questa prova.

Nativo: output_send origin79575,49020/345600byte e4record, elapsed1001ms su
1000ms; retirement della sorgente dopo2661312ms di vita,79443frame main,
39720archive e52960analysis. Nuovo epoch2; alle06:26:48 discontinuity H265
443->448/delta5, seconda retirement. Alle06:26:51 inizia lo stop dei servizi
per il deploy. Il riavvio/azzeramento contatori non cancella le due ricorrenze.

Il deploy ufficiale f5dccb2d era gia' nella preparazione su disco; non era ancora
attivo e il suo trial/soak non era iniziato. Nessuna interruzione/ripetizione del
deploy, nessuna modifica a soglie o profili. La qualificazione della milestone
richiede comunque il nuovo soak canonico continuo; il presente ticket del goal
generale resta aperto e non viene chiuso automaticamente con la milestone.

Lettura successiva, runtime fermo: memoria disponibile2645MiB, swap usata47,5MiB
su2GiB, tre campioni senza swap-in/out corrente. Non e' telemetria coeva al
guasto e non ne prova la causa. Nessun errore kernel esplicito nella breve
finestra raccolta, soltanto chiusura/riapertura delle istanze VPU.
Evidenze private persistenti in
`<private-workspace>`:
`test-board-development-tail-before-official.log`,
`test-board-snapshot-failure-phases-r2.log`, `test-board-update-pressure-readonly.log`.

## Aggiornamento11settembre09:50UTC

### Ricorrenza11settembre16:09:37UTC, release completa31a

Trial precedente PASS, poi timeout nell'avvio definitivo: header_receive
1083919us/budget1000000us, header1082955us,1record/24byte, zero payload;
authority2check98us/max57us, threadCPU4347us, lastdelivery118/epoch1.
Stack nel controllo deadline dopo il ritorno channel.receive. Il broker
conferma2delivery completate/9idle e ritira la connessione alla chiusura peer;
non e' prova di SHA lento o caduta USB. Ripresa GIL/scheduler e attesa del
record sono da distinguere, non attribuite. Errore output_send16:09:46
successivo marcato lifecycle=stopping, NON causa originaria.
Updater fallito e servizio fermo con inhibitor dopo host_committed; nessun
soak31a. Prove nel pack31a/test-board-31a-first-runtime-failure.journal.
Il risultato smentisce la chiusura del problema con i soli hotfix precedenti.

Root5fb7: timeout08:35:56UTC in header_receive5.058s/budget1s e successiva
publication sorgente3.171s. Poi08:43:33header_validation18.048s;09:00:59
publication Go10.270s;09:06:17nuova retirement. Epoch5/Tracker2490176 successivo
non cancella gli errori azzerando i contatori. Eventi in resume-stall-events.log.
Primo errore precedente al dry-run corpus08:36:05, non attribuirlo a quel probe.
Nel secondo backup08:58:25-09:01:25:28allocstall e17compattazioni fallite;
correlazione temporale con lo stallo09:00:59, NON prova unica della causa.
Traccia successiva600campioni/300s senza copie/build: nessuna nuova attesa
allocazione/compattazione/swap, stessoPID e nessun read_error, circa18.5FPS.
Non soak; include180sprofilazione nonblocking. CPU/GIL, memoria e I/O restano
cause da distinguere; non qualificare il sistema da un solo campione quieto.

Correttiva separata, SDK locale e in test test board, non ancora attiva: mantiene GIL
solo durante il controllo nativo strettamente non bloccante dei pidfd; attese
socket e payload continuano a rilasciarlo. Guard C sul provider reale RED->GREEN,
30SDK/439Tracker PASS. A/B3200delivery sintetiche autentiche: sotto contesa GIL
simulata33.261->31.664ms,CPU-2.7%; quasi invariato senza contesa. Beneficio del
trasporto, non prova che risolva le pause di secondi osservate anche in Go.
Nessun controllo authority, framing, SHA, deadline o quota eliminato.

## Aggiornamento11settembre07:50UTC

test board34465: alle06:58:33UTC la capture front termina con
`uvc_dqbuf_enodev` dopo20221017ms; successiva cattura epoch2. Evento USB
distinto dal seguente timeout, nessuna attribuzione a SD o saturazione CPU.
Alle06:59:24UTC: snapshot broker3.604ms, send1.000236s,
5record/65984wire/65360payload. Client elapsed4.534768s su budget1s,
header26.433ms, payload4.505142s, authority10check/4.497740s
(max4.495750s), CPU28.224ms,4record/49020payload validati.
Tracker si ricrea (PID2359553). Causa della lunga attesa non ancora attribuita;
non confondere re-enumerazione USB con errore di trasporto locale.

Candidato ff1e7810 pronto, NON ancora attivato a questo aggiornamento:
ricezione payload delivery/ROI con singola chiamata C sul provider autentico,
scratch e buffer del caller, senza code/thread/allocazioni aggiuntive native.
Controlli peer/pidfd, lunghezza/digest/offset perrecord, deadline assoluta unica,
SHA finale e rifiuto dopo tardiva ripresa Python invariati. Regresso esplicito
per native concluso in tempo ma ritorno Python oltre deadline.
460test locali e449onboard PASS; integrazione decoder/SDK/crop/Registry e race
PASS. Misura FPS/CPU/timeout live e qualifica ancora aperte: non e' una
correzione dimostrata della pausa4.5s. I test sperimentali isolati non provano
un guadagno end-to-end. Prove `batch-*` nel pack persistente gia' indicato.

Attivazione ff1e iniziata07:51:56UTC su test board tramite controller misurato,
ancora in copia verificata08:02UTC. Regresso nativo FAPD aggiuntivo8PASS sotto
ASan/UBSan; LSan escluso per il processo Python, non prova completa assenza
leak. Matrice decoder/crop/Registry estesa a1920x1080, insieme a720p e16x16,
MJPG/H265/H264 PASS12.855s; solo fixture adeguate al volume della nuova prova,
nessuna quota o deadline di produzione aumentata.

## Evidenza live, 10 settembre 2026

test board candidato2c277b7f, brokerdfae8dc6. Alle19:00:57.755UTC il Tracker1552126
scade in header_receive: elapsed1002047us/budget1000000us, zero record/byte,
thread_cpu981us. Il broker riporta snapshot2994742us, totale2994957us,
budget999900us, phase=header/authorized_write, zero record confermati;
delivery22219->22220, origin27923, source/publication epoch1.
Il Tracker riparte19:01:00.901UTC (PID1576182), riaggancia19:01:07.658UTC.
Broker/native/supervisor e captureepoch invariati.

Lo snapshot comprende authority/resource admission, copia provider, parsing e
validazione. Il log aggregato non distingue ancora queste sottofasi: NON e'
dimostrato se sia hash nativo, contesa, scheduler o altra attesa. Il client e'
locale: la rete esterna non trasporta questo frame. Non aumentare la deadline.

Verifica successiva303.675s:17.571fps Tracker contro19.99analysis, nessun nuovo
errore/PID change, Tmax87.013C. Non cancella il riavvio e non e' soak.

## Chiusura

Correttiva locale10settembre: riprodotto un difetto indipendente nell'ultimo
check authority: puo' terminare dopo cancellazione e invocare ugualmente il
consumer, prima del rifiuto nell'epilogo Resource.Borrow. RED consumer_called=true;
ora check della deadline prima/dopo la preparazione e prima del consumer.
Diagnostica error-only distingue authority, resource, provider, header, validate,
consumer e release, senza pixel/code/log per-frame. Causa dei2.995s ancora NON
attribuita e correzione NON ancora distribuita: il ticket rimane APERTO.

Test locali: regressione RED->GREEN, suite mirata delivery PASS, race su
frameingress/framedomain/framewire e provider PASS. Suite broker completa PASS
dopo correzione di un difetto nella raccolta stderr terminale (Cmd.Wait chiudeva
StderrPipe prima del drain; ora reader di proprieta' broker, chiuso a fine drain)
e della fixture shell cooperativa (TERM prima del fork sleep rinviava il trap).
Regressioni stderr20ripetizioni con race PASS; invariati timeout di produzione
e della fixture. Nessuna equivalenza con qualifica integrata test board/soak.

- Riprodurre/attribuire la sottofase; log errore bounded, niente pixel/segreti.
- Correttiva con ownership/quota/current-only, integrita' e isolamento consumer
  lento. Nessuna coda illimitata, fallback implicito o scadenza estesa.
- Regressioni deadline/contesa/retirement, race e prestazioni; hotfix nel Git.
- Verifica reale e soak canonico; dichiarare limiti di attribuzione.
  Nuovo PID con contatori azzerati non e' PASS.

Prove persistenti esterne:
`<private-workspace>`,
`check-1947-incident.log`, `check-1947-recovery.log`, `check-1947-report.md`.
# Aggiornamento10settembre22:05UTC: ricorrenza distinta dopo hotfix

Il candidato5f29bc9e, broker0ab20fc8, e' attivo21:19:34UTC dopo test race
onboard/build/promozione/misura esatta. Il21:25:17UTC nuova consegna6456/origin8154
si interrompe nel payload: client19record/295360wirebyte/294120payloadbyte
su345600, budget1s/elapsed1002052us, threadCPU9875us, authority1769us.
Broker snapshot5762us, invio2078532us, transazione massima2063641us.
Non attribuire questa ricorrenza a snapshot/copia/SHA o Tailscale: la consegna
e' IPC locale, il ritardo e' successivo allo snapshot. Tracker si ricrea;
supervisor/broker/capture e source epoch restano gli stessi.

Ulteriore gap riprodotto localmente: Resource/Buffer Borrow scartava l'errore
del callback quando scadeva il contesto, perdendo anche i diagnostici interni.
Corretto con catena errori congiunta senza accettare risultati tardivi o
allungare timeout. Due regressioni RED, race framedomain/frameingress count3
GREEN. Questa seconda modifica NON ancora distribuita. Causa della lunga
attesa autorizzazione/invio da attribuire, issue ancora aperta.
Prove: pack persistente crop-orientation/core-live-incident.log e
core-live-resource.jsonl (25campioni121.049s/18.191fps/Tmax89.162C).
