# Invio FAP interrotto: causa persa e worker uscito con stato riuscito

Stato: **aperta; correttiva diagnostica e stato di uscita testati localmente,
compilati e attivati su test board; postflight iniziale PASS, stabilita' ancora aperta**.
La causa iniziale dell'interruzione e la stabilita'
hardware restano da verificare. Distinta dall'issue di enumerazione USB front.

## Evidenza aggiuntiva14settembre: causa capture-integrity non riassunta

Durante il controllo successivo allo switch ottico front->overview->front,
release qualificata054b59c0-c3d8b4b86353 invariata, broker1797832: due eventi
13:58:48 e13:59:01UTC, incident1797832-3/1797832-4, native1829062/1830110.
Entrambi registrano `interframe_capture_discontinuity format=h265`, sequence
7119->7121 e253->256, `admitted=true action=retire_source_before_publication`.
Cleanup SDK confermato, exit64; riepilogo broker invece
`reason=child_exit_without_terminal_reason`. Recovery automatico fino a
epoch5/native1830159; ultimo stato14:03:35UTC valido, routingfront30/20/15.
Non e' prova di stabilita' ininterrotta o di guasto USB, ne' un errore di invio
FAP dimostrato. Estendere la futura verifica della copertura diagnostica anche
al motivo capture-integrity, senza confonderlo con source-send e senza
cambiare le guardie che impediscono la pubblicazione di frame incompleti.
La causa ultima delle discontinuita' resta da isolare; nessun fix di codice
o deploy effettuato per questa aggiunta. Prove private
`test-board-source-switch.mbYCLM2f/{post-switch-recovery.log,native-exit-details.log,
postflight.json}` nel PAL workstation; la
[issue esposizione](2026-09-14-front-dark-daylight-automatic-exposure-not-verified.md)
conserva il contesto del test, distinto da queste interruzioni.

## Delta 10settembre locale: attesa nello store e riepilogo terminale

Dopo la prova SQL rollback-only del Registry, il runtime6ec invariato e'
ripartito22:31:17UTC. Nuovo guasto22:32:43UTC: `output_send/io_deadline`,
source_epoch1/origin1378,4record/49020byte,elapsed1001ms/hash3ms. Il ricevitore
aveva gia' completato il frame nativo corrispondente:192record/3119252byte wire,
payload3110400,assembly14.922ms/integrity17.387ms; poi
`phase=publication`,publication2341.171ms/elapsed2384.849ms,
`frame ingress stage=surface_store: canceled`. Il log non separa ancora
attesa authority/domain/store/copier/finalizzazione: questa causa ultima
resta aperta. Non attribuire il guasto a filesystem, USB o termica senza prova.

Il riepilogo ha nuovamente perso la causa (`child_exit_without_terminal_reason`),
pur drenando stderr. Riprodotto localmente: `brokerChildTerminalFailureReason`
non riconosceva il record comune `[video-broker][frame-source]`. Correzione
in `internal/broker/causal_delivery.go` e `broker.go`: evento/fallimento/azione
terminali verificati, stage/peer bounded conservati, niente stderr arbitrario
nel reason e nessuna quarantena USB per un errore IPC. Vale per ogni adapter
che usa il contratto nativo comune, non solo RDK X5.

RED del matcher, poi race mirato1.023s PASS; test reale child stderr/exit64
verifica riepilogo causale e assenza di quarantena USB. Integrazione race
frameingress2.676s/internal-broker25.992s/cmd6.519s PASS. Fix **non ancora
attivo** a questo checkpoint; incluso nella build mirata8file
`nemesi-broker-build-20260909223457.service` insieme alle guardie deadline.
Script/ricevute/log nel pack persistente indicato dal handoff; non chiudere
la issue per il solo miglioramento diagnostico.

### Separazione delle attese nello store: delta locale, non nel candidato95

I log del6ec hanno ristretto il guasto a `surface_store`, ma quel tempo include
authority/domain, copia e finalizzazione. Nuova diagnostica in
`frameaccess/publication_diagnostics.go` e
`framedomain/publication_diagnostics.go`, integrata nei percorsi correnti:
admission/copy/commit_wait/finalization per lo store;
begin/store/current/finish per il dominio. Tempi monotoni in microsecondi
dal clock gia' iniettato, fasi non raggiunte=-1. Record costruito sugli errori,
nessun log per frame sano, nuova coda, retry, modifica timeout/admission o
assunzione sull'acceleratore; errors.Is/errors.As e codici semantici preservati.

RED riprodotto per diagnostica assente; la prima suite ha respinto correttamente
un import diretto di `time` nel core. Corretto usando la porta Clock corrente;
il controllo architetturale e' esteso anche al nuovo file, non indebolito.
Quattro regressioni coprono codice errore/recupero memoria, fasi annidate,
tempi esatti/assenti/clock regressivo e zero allocazioni del record diagnostico
in caso sano. Race su frameaccess/framedomain/frameingress/internal-broker/cmd
PASS; dettagli nei log `surface-publication-stages-{red-final,integration,
integration-final}.log` del pack persistente. La prova temporale misura attesa
wall, non CPU o tempo di esecuzione hardware.

Questo delta NON modifica il candidato95 gia' in promozione. Non dichiararlo
onboard ne' ripetere la promozione avviata per includerlo silenziosamente.

Diagnostica parallela in sola lettura durante quella promozione: nessuna quota
CPU del servizio indicata da systemd e memory.events senza OOM. Campione30.029s:
swap-in/out di sistema invariati, broker e Tracker con0major fault, RAM
disponibile~1.3GB; Tracker309577minor fault, broker RSS invariato128424KiB.
Questo campione non prova assenza generale di leak/stalli e non e' steady-state
ne' A/B: l'updater stava copiando la release. La sola presenza di pagine in swap
non dimostra che abbiano causato quel timeout.

## Evidenza integrata test board

9settembre2026, copia di sviluppo c8ca34ec su baseline9ffc. Front2bc5:0529
assente; selezione esplicita single-overview tramite API routing:
piano96 ->97, operazionefap-apply-1788970399193683391, successo16:13:25UTC.
Nessun nuovo deploy, profilo sorgente/cap termico o dato storico modificato.
Live1280x720@30, analysis640x360@20, archive640x360@15 AVBR750 dal catalogo.
Il vecchio owner overview115797 termina con unmap4/4,close0,rilascio SDK
confirmed; nuovo owner123498 e Tracker123571 avviati. Questo passaggio
non ha preservato la cattura: il vincolo FAP sui FD ereditati resta aperto.

Alle16:16:25UTC il nuovo worker registra `source_surface_send`, published3396,
send_stage5, bytes49020,records4,offset49020; poi chiude le risorse con successo.
Il log worker riporta erroneamente operation_status0/exit_status0. Alle16:16:29
il broker osserva source_receive/peer failure, poi
`child_exit_without_terminal_reason`. Tracker termina per binding ritirato.
Recovery automatico avvia owner127526/source_epoch2 e Tracker127539: il vecchio
binding non viene riutilizzato. Nessuna nuova disconnessione USB overview
dimostrata da questi log; nessuna prova che fosse un timeout anziche' un altro
errore di trasporto. La fase del send, da sola, non distingue la causa.

Il postflight nega correttamente la stabilita' per task6 exits1.
L'osservatore ROI180s successivo ha0candidati: **NON PASS** del percorso ROI.
Campione60.052s durante l'avvio della nuova generazione:0 ->1080frame,
read_errors0, temperatura80.27-80.42 ->81.30-81.59C. Non e' una misura steady
state dei20fps. Successivo preflight compilazione registra circa85.7C.

## Correttiva locale

- `frame_source_linux.c/.h`: primo errore conservato, log terminale con codice
  originale, fase del peer, sorgente/epoch/origin, progresso esatto dell'invio,
  payload, budget e tempo totale/integrita'. Tempi non disponibili valgono-1.
  Nessun log per frame, nuovo worker, coda, retry ambiguo o cambio timeout.
- `rdkx5_v4l2_mux_bridge.c`: un precedente rc0 non nasconde piu' un fallimento
  FAP; la chiusura verificata produce exit64, quella incerta continua a65.
  Il codice originale del guasto e' conservato anche nel log di cleanup.
- Regressione su source sender C reale con trasporto iniettato:18casi
  (header/chunk, superficie/output,deadline/unavailable/unauthorized).
  Vecchio codice RED per assenza del record diagnostico; nuovo PASS ASan/UBSan,
  nessun retry dopo invio incerto. Suite native completa e Go race/integration
  frameingress14.253s/cmd9.540s PASS. Le fixture non attestano USB o acceleratori.

## Residuo e prossimo passo

Aggiornamento9settembre19:18UTC: il nuovo binario ha riprodotto due veri
source_surface_send io_deadline,19:12:34/19:14:31, rispettivamente49020 e
359480byte trasmessi; budget1000ms, elapsed1001ms, hash13/14ms. Rilascio
confermato ed exit64 corretti, ma la causa della mancata ricezione resta aperta.
Recovery a epoch3 riuscito; postflight fallisce correttamente task6 exits2.
Questi guasti avvengono nel runtime ripristinato, non nel solo harness.
Distinto il timeout100ms della richiesta full-frame, per cui la misura isolata
ha individuato SHA standard~89ms contro provider corrente~13-14ms. Non dedurre
che questa ottimizzazione da sola risolva anche il trasporto da1secondo.

### Diagnostica ricevitore distribuita nel candidato test board il9settembre20:15UTC

`frameingress/source_receiver.go` accompagna gli errori con fase
workspace/header/payload/integrity/publication, contatori limitati alla singola
operazione, byte accettati e assemblati, dimensione dichiarata, riferimenti
del record e tempi distinti. Non confonde byte fisicamente letti ma rifiutati
con record autenticati/accettati. I tempi assenti valgono-1.
Cause peer classificate solo dai sentinel pubblici: nessun dettaglio privato
del provider viene aggiunto al testo; errors.Is/errors.As conservati.
Nessun log per frame riuscito, coda, retry, cambio deadline o quota.
Test deterministici RED prima del fix per diagnostica assente; GREEN su
header/partial/integrity, stato tra frame successivi e causa privata mascherata.
Suite SourceReceiver race PASS1.145s; race+nemesi_integration su frameingress
11.702s e cmd/video-broker8.115s PASS sul codice finale.
Distribuita insieme al provider SHA delle estrazioni nella copia di sviluppo
522eb799/79edc59079fc, binario brokerca0b6aec; misura esatta4ae3e8d4,
42file/hotfix preservati, postflight15s/accesso e binding FAP PASS20:16:08.
Il precedente5223776e ha riprodotto ancora source_surface_send io_deadline
19:54:26 durante la copia; non attribuirlo al nuovo broker ancora inattivo.
Non e' risolta la causa iniziale dal solo superamento del postflight.

Analisi statica: observeCausalDelivery mantiene rt.mu mentre il completamento
dell'ammissione camera salva lo stato con fsync di file e directory su SD;
lo stesso writer e' chiamato dal watchdog sotto mutex. Alle20:15:27 una
osservazione /proc-only vede un fd .camera-mode-health aperto e un thread in
jbd2_log_wait_commit, ma soltanto1campione su356 in90s: durata e causalita'
del timeout non dimostrate. Non introdurre correzioni o soglie per supposizione.
Resta anche da coprire la race diagnostica nel chiamante: receive_failed viene
loggato solo quando childCtx non e' ancora cancellato, quindi il primo errore
potrebbe restare nascosto se il worker termina prima del ricevitore.

Nuovo residuo riprodotto sul candidato522eb799:20:18:26UTC,
delivery_session_send/context deadline e Tracker delivery total deadline exceeded,
last3440. Lo stack SDK delivery.py:100 colloca l'errore al controllo dopo header.
Il Tracker riparte405994->410163/generazione2, mentre broker405694 e cattura
405914/epoch1 restano gli stessi. Non chiamarlo source timeout o guasto USB:
e' il canale delivery broker->consumer; causa precisa ancora da isolare.
Il consumo reale Registry di ROI head352x394/body1000x764 e' passato nello
stesso candidato, ma non chiude questa instabilita'. Nessun timeout aumentato.

### Diagnostica delivery successiva: compilata/testata onboard e attivata

`frameingress/delivery_diagnostics.go` e `delivery_sender.go` distinguono
workspace,snapshot,metadata,header,payload,idle e autorizzazione prima/dopo invio;
conservano errors.Is/As e contano soltanto scritture confermate, non byte fisici
incerti. SDK `delivery.py` registra la prima operazione fallita prima del cleanup:
fasi di ricezione/validazione/integrita', record ricevuti, payload dei chunk
validati distinto dall'integrita' del frame, riferimenti esatti e tempi
wall/thread CPU. Nessuna stringa privata del provider o pixel nel nuovo log.
Un handler di logging difettoso non impedisce la chiusura; un lettore gia'
ritirato non ripete il log. Deadline1s, binding, quote e zero retry invariati.

RED semantico per diagnostica assente, poi GREEN; suite completa Go race e
29test Python (9SDK standalone,20integrazione Tracker) PASS. Verificati errore
dopo header, trasferimento parziale, pixel corrotti, scadenza anche dopo hash
corretto, cleanup fallito e logging fallito. Non e' ancora una prova hardware
del nuovo delta, una misura del suo overhead o una correzione della causa.
Ultimo campione conservato20:38:31: owner405914/epoch1,Tracker410163/gen2,
20395frame/errors0,temperature88.918/88.674C; non cancella il guasto20:18.

9settembre20:53: primo preflight fallisce per systemctl show oltre4s; seconda
lettura autenticata PASS,34366frame/errors0,stessi processi/epoch,89.211/88.967C.
vmstat5s mostra I/Owait7-31percento, niente swapin/out nei delta: non dimostra
che la SD sia la causa unica del timeout o che sia difettosa.
Il chiamante ora conserva il guasto anche se childCtx e' gia' cancellato;
action=join_capture distingue quel caso dal retirement richiesto dal ricevitore.
Stop normale silenzioso, errori originali conservati,7casi deterministici PASS.
Suite race+nemesi_integration frameingress12.377s,internal/broker26.964s,
cmd/video-broker8.587s PASS. Build10file+solo broker eseguita su test board con test
Go/SDK onboard PASS,max82.665C; vecchia misura9273entry verificata e runtime
ripristinato20:59:40. Candidato manifest6ec8fe81/binariocf653906.
Promozione canonica21:01:50-21:22:12,unit nemesi-broker-promote-20260909210144,
terminale0. Nuova root6ec8fe81/b13a9f8a938b,misura67748a12,
9277entry/552853653byte/50file-hotfix verificati. Postflight15s,accesso e binding
autenticati PASS21:23:42. Osservatore300s senza nuovi eventi nel filtro FAP,
terminato124 per limite esplicito: NON errore del servizio e NON soak PASS.
La diagnostica e' attiva, ma la causa del timeout e' ancora aperta. Campione
successivo60.054s:Registry42.3% di un core/984245read syscall,Tracker49.24%
e634941minor fault/0major;17.135frame letti/s, non inferenze effettive.
Questi delta non provano leak, difetto SD o causalita' del timeout. Conservarli
per correlare un eventuale nuovo guasto, senza alzare deadline o soglie.

### 9settembre21:49UTC: nuovo guasto prima dell'header e deadline consumer

Sul6ec8fe81 dopo circa27minuti, source_epoch1/origin31658:
`source_surface_send`,code-5,peer_stageio_deadline,send_stage3(header),
0byte/0record inviati,payload3110400,budget1000ms,elapsed2127ms,hash15ms.
Ricevitore broker:phaseheader,0record,elapsed125159us,peerunavailable;
Tracker:header_receive,1590us,PeerError. Non e' lo stesso errore delivery
consumer delle20:18: la chiusura qui parte dal source transport. Nessun
nuovo guasto USB provato e causa del ritardo ancora da isolare.
Cleanup/exit64 e recovery automatico;22:00:57 lettura autenticata PASS,
owner516679/epoch2,Tracker516681/gen2,11387frame/read_errors0,
89.309/89.749C,liveinattivo. Recupero riuscito NON equivale a stabilita'.

Un test deterministico ha inoltre provato un gap nel contratto del broker:
`acceptVerifiedOutput()` accettava il nil del consumer anche dopo la deadline.
Il contatore Outputs avanzava e il ritardo in publication non veniva riportato.
Correzione locale: dopo consumer riuscito ricontrollare deadline e binding;
se consumer ha un proprio errore conservarlo, non sostituirlo con il timeout.
Niente retry di side effect gia' possibili o aumento del budget. Test RED
`got <nil> want context deadline exceeded`, poi race mirato GREEN1.135s,
con fasepublication,0successi/0drop,1fallimento e receiver ritirato senza retry.
Coperta anche la retirement durante il consumer. Delta non ancora distribuito;
dimostra un difetto reale, non che sia l'unica causa del guasto hardware sopra.
Receive ricontrolla inoltre il budget specifico del frame dopo il borrow
legato al parent: idle provato invariato, completamento tardivo non contato.
Race+integrazione finale:frameingress11.841s,internal/broker cached dalla suite
25.939s,cmd/video-broker8.265s PASS. I primi errori di socket Unix dovuti al
percorso temporaneo lungo sono stati risolti usando `/tmp` per le sole socket
e build rigenerabili, senza indebolire le prove; log e sorgenti persistenti.

### Stato osservato dopo l'attivazione nativa

Build nativa completata16:46:52UTC, massimo80.809C dopo stop/cooldown manutentivo,
linkage PASS e ripresa vecchio runtime16:47:14. Delta esatto3sorgenti+test+binario
V4L2 `105fa9ea`; manifest modulo `5223776e`. Promozione canonica prebuilt avviata
17:07:51 tramite Tailscale dopo uscita del PC dalla LAN. Nessun Go/npm ricompilato;
backup e prove sotto PAL operator-backups, non dentro la release misurata.
Promozione conclusa rc0: nuova root5223776e/f37f15e2d9d8, misura esatta
9268entry/552790643byte SHAee0618da PASS,12hotfix verificati. Postflight15s,
accesso e FAP binding PASS17:21:26, owner205940/epoch1 e Tracker206005,
671frame/read_errors0, circa86.5C. Campione successivo60.021s:17.39frame/s,
read_errors0 e generazione stabile;17:24:23 totale3753frame e circa86C.
ROI180s0candidati NON qualificata. Nessun nuovo errore FAP nel breve intervallo
osservato dopo il riavvio; non e' soak o recovery dimostrato della causa iniziale.
Ulteriore osservatore journal600s terminato senza evento (timeout124 atteso);
17:49:45 Tracker30676frame/read_errors0, stessi PID/epoch1 e circa87C.
Temperatura campionata17:42:31 fino89.602C: qualifica ancora da dimostrare,
non dedurre il massimo continuo dai campioni o spiegare il calore per ipotesi.
La vecchia versione ha riprodotto anche delivery_session_send/deadline16:48:21,
source_surface_send17:05:40 (539220byte/34record; receive_idle_deadline del broker),
poi altri recovery17:12. Il primo precede la copia del nuovo snapshot:
la pressione I/O non e' dimostrata come causa unica. Questi non sono guasti
del nuovo binario, ancora inattivo durante l'osservazione.
Riprodurre l'interruzione con il log
completo per discriminare hash, attesa del ricevitore, deadline e perdita peer.
Non aumentare soglie o timeout per fare passare il test; non chiamare risolta
l'interruzione soltanto perche' la diagnostica e l'exit status sono corretti.

Evidenze persistenti: `<private-workspace>`
`uvc-development-single-overview-{preview,apply,start,postflight,resources,roi}.log`,
`uvc-development-overview-fap-restart.log`,
`frame-source-failure-{red,green,native-suite,go-integration}.log`.

### Audit statico durante la promozione Event Captioning, 10settembre locale

`frameaccess.Store.tryPublish` libera il mutex prima di `CopyNV12` e lo
riacquisisce alla finalizzazione. `Binding.begin/current/finish` attraversa
anche autorita' di processo e mutex del dominio: restano fasi distinte da
misurare, non una prova di quale abbia atteso2341ms sul device.
Il provider Linux verifica il pidfd con poll a timeout0, massimo4tentativi su
EINTR; il clock del runtime e' `monotime.System` basato su `time.Since`, non
un esplicito ClockGettime per ogni riga copiata. Questi fatti non escludono
ritardi di scheduling/I/O, ne' dimostrano la causa dell'episodio.
Nessuna modifica speculativa a lock, frequenza dei controlli o deadline.
La diagnostica Store/Domain resta locale e fuori dai payload95/dc59.

### 9settembre23:43UTC: dc59, gap tra header e ricezione payload

La promozione dc59 aggiorna soltanto Event Captioning; broker7d8a779d invariato.
Delivery fallisce dopo5record/65360byte entro circa1000ms. Reader Tracker:
3801555us wall contro5627us threadCPU, header_wait5848us, payload_us34;
header448byte ricevuto ma0byte payload validati. Poco dopo source_surface_send:
0byte/0record,3771ms contro budget1000,hash15ms. Il riepilogo ora conserva
`frame_source_failure:source_surface_send:io_deadline`; cleanup exit64 confermato.
Due uscite Tracker, poi recovery epoch3. Snapshot23:43:58 circa85.2-85.45C,
non misura termica dell'istante esatto del guasto. Stabilita' NON qualificata.

Gap wall tra header e payload non attribuibile a GIL, allocazione, SD o scheduler
senza misura. SDK locale aggiunge tempi header_validation/allocation e CPU
allocazione, solo nel log d'errore;3regressioni RED->GREEN,12test SDK PASS,
race non-cache5package PASS. Accorpare con Store/Domain nel prossimo delta;
non e' ancora sul dc59 e non modifica timeout/contratti/fallback.

Campione VM successivo30s: nessun incremento compact/allocstall/swap; Tracker
315708minor fault/0major, RSS stabile. Snapshot ION riporta orphaned0, ma buffer
client condivisi/importati impediscono somme semplici; nessuna prova generale
di assenza leak o di causa termica. Conservare anche i contatori cumulativi,
distinguendoli dai delta, senza attribuirli retroattivamente al guasto.
Log nel pack persistente: `event-outbox-startup-journal.log`,
`event-outbox-vm-stall-sample.log`, `fap-kernel-stall-baseline.log`,
`fap-ion-memory-owners.log`, `delivery-allocation-go-integration.log`.

### 10settembre00:19UTC: deadline del controllo peer, correttiva SDK

Riproduzione deterministica di un ulteriore gap: `DeliveryReader._current`
controllava la deadline PRIMA di `channel.current()`, non dopo. Se il controllo
peer completava tardi, l'ultima verifica poteva restituire pixel gia' oltre
budget. Dopo l'header, la stessa attesa era attribuita al successivo payload
senza tempi del controllo. Tre regressioni inizialmente fallite, poi verdi.

Corretto il controllo prima/dopo, incluso l'accesso iniziale, senza nuovi
timeout/retry. Log d'errore con `authority_checks`, `authority_us` cumulativo e
`authority_max_us`: wall time, non CPU o prova che la verifica kernel sia lenta.
L'errore originale del provider resta autorevole anche se la chiamata e' tarda.
`FrameAccessClient` aveva lo stesso gap per le ROI: due regressioni RED, poi
corrette; non invia nuove richieste ne' restituisce crop dopo un check tardivo.
La preimage di `frame_access.py` coincide con test board via SHA016cb741229a58d6,
quindi non vengono inglobate modifiche sconosciute del file.

Suite SDK completa18test PASS, cinque package Go con race/non-cache PASS.
Questo prova i difetti di deadline, NON la causa ultima dello stallo test board;
la nuova misura del controllo peer deve ancora essere osservata nel runtime.

Prima build onboard00:09:13: Go compilato (SHA8be3ee298975c468), poi suite
fallita per socket Unix oltre il limite del pathname e ulteriori timeout.
Nessuna promozione; dc59 originale verificato e ripristinato00:14:21UTC.
Il nuovo job00:19:10 usa `/tmp` solo per socket/build usa-e-getta e ripete
integralmente gli stessi5package, senza esclusioni o aumento dei timeout.
Riutilizzo del binario subordinato al confronto esatto di TUTTI gli input
non-SDK; quattro file SDK/test aggiornati in una nuova area, ricevute precedenti
immutate. Dati, soglie e profili non cambiati.
Log: `delivery-authority-deadline-{red,green,go-integration}.log`,
`frame-access-authority-deadline-{red,green}.log`,
`phase-final-sdk-go-integration.log`, `phase-diagnostics-build-failure.log`.

### 10settembre00:32UTC: residuo isolato nella fixture nativa sanitizzata

Con pathname Unix corto sono PASS onboard frameaccess/framedomain/frameingress/
internal-broker. Il limite complessivo180s includeva compilazione/preparazione:
il completamento separato mantiene timeout Go90s e tutte le prove precedenti,
ma registra un solo fallimento cmd: `TestNativeFrameCaptureMuxLifecycleAndRestart`.
Dal bind37.452 al kill42.455 del medesimo minuto passano i5s del contesto;
pubblicazione, correlazione, verifica pixel e ROI sono gia' superate. Fallisce
`cmd.Wait()` dopo l'invio dell'ordine di chiusura. Non e' ancora provato se
attenda il codice sorgente, l'uscita/sanitizzatore o altro; nessuna inferenza
di guasto camera o SD da questa fixture sintetica.

Tracce test Go/C aggiunte e PASS locale2.330s: await_release, source_close_begin,
source_close_complete, main_return; wchan e figli del processo se attende oltre1s.
La deadline originale5s e Address/Undefined sanitizers restano invariati.
Nuova diagnosi onboard mirata; nessuna promozione o sostituzione della release.
dc59 esatto/65pin ripristinato00:32:29UTC. Log JSON originale completo
`phase-completion-cmd-tests.jsonl`, prova locale `native-exit-diagnostics-local.log`.

### 10settembre01:03UTC: fixture corretta e candidato pronto, non attivo

La diagnostica successiva mostra `source_close_complete` e `main_return`
prima del kill. Il processo era in ptrace-stop con un figlio dello stesso
eseguibile sanitizzato impegnato nella scansione finale. Separati nel TEST
il limite funzionale5s (incluso ack esplicito della chiusura), il limite30s
del processo sanitizzato e il retirement5s dopo il reap. ASan/UBSan e verifica
leak non disabilitati; resta obbligatoria uscita0. Nessuna deadline runtime
o soglia termica modificata, nessuna inferenza di difetto SD dalla fixture.

Onboard due cicli PASS: uscita post-main5490/5483ms, test22.50s; massimo
osservato circa81.102C, ripristino esatto dc5900:53:40UTC. Prova JSON SHA
`08fb575d350014b7284c7b959f742c350b6289fc47eb07bd673905ddb6fddf07`;
locale race mirato2.418s PASS. Riutilizzati quattro package PASS e57altri
test cmd PASS senza skip;18test SDK ripassati onboard dal finalizzatore.

Candidato14file e binario8be3ee29 preparati con ricevuta nuova01:03:22UTC,
manifest b0211cc8. Non promosso; dc59 rimane attivo. La causa dello stallo
FAP reale e la stabilita' del nuovo candidato richiedono ancora la verifica
integrata: il PASS della fixture non chiude questa issue.
