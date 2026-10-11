# deployed board 24 settembre: la registrazione video si ferma alle 11:38:54 e non riparte

Diagnosi read-only su `nemesi-deployed-board-0655` (release
`dev-1788795351463040430-9f6ced8909bb83c9-9987d5c89902`, avvio unit
`2026-09-23 17:56:51`). Richiesta dell'utente: "verifica come mai deployed-board non
registra piu".

**Stato: aperta — deployed board recuperato operativamente, correttiva strutturale in
qualifica su test board, non ancora rilasciata su deployed board.**
Il 24/09/2026 deployed board è stato riavviato tramite il percorso amministrativo canonico
dopo la verifica della misura esatta della release e dell'assenza di una
transazione attiva. Alle 16:20 CEST il recorder ha finalizzato il segmento
`20260924-141000` con 9.018 frame video; il broker riportava archive 15,04 fps
e analysis 19,61 fps, senza nuovo fault della cattura dal riavvio. La release
su deployed board è ancora quella precedente (`dev-1788795351463040430-9f6ced8909bb83c9-9987d5c89902`):
questa continuità non qualifica la nuova correzione. Il checkout esaminato è
`de1914afe8b4` più le modifiche non ancora consolidate qui descritte.
Restano da allegare log originali, boot ID e invocation ID normalizzati in UTC.
I numeri di riga seguenti descrivono il checkout esaminato, non sono stabili.

### Correttiva in qualifica (24/09)

- La consegna `analysis` usa ora un publisher nativo a due slot immutabili:
  hash e socket send non trattengono il ciclo camera/encoder/archive. La
  sessione parzialmente inviata viene chiusa senza riutilizzarla; i drop sono
  contabilizzati. Anche una mancata delega/allocazione del solo mittente
  degrada `analysis` senza abbattere archive; il watchdog distingue assenza
  prolungata di frame da semplice FPS ridotto.
- Il mittente asincrono riceve dal provider una delega con FD duplicati e
  identità verificate, non una copia della struttura mutabile del proprietario.
- La pulizia del FAP continua dopo la scadenza del chiamante; una seconda
  osservazione riconcilia **la stessa** operazione. Il join del reader ha un
  limite e trattiene l'ammissione se non termina. Un rilascio del provider
  pendente o realmente non confermato scrive un guard atomico nel PAL di stato;
  un nuovo processo non lo cancella automaticamente e rifiuta l'ammissione.
  Solo l'esito positivo della stessa pulizia rimuove il guard pendente; un
  guasto terminale richiede prova di reset hardware indipendente.
- Il bridge che ha già rilasciato esplicitamente le risorse evita la seconda
  pulizia implicita dei vendor handler `atexit`, probabile causa del processo
  rimasto vivo dopo `sdk_release=confirmed`.
- Test locali Go (broker, cattura, provider Linux), `go vet` e test nativi
  ASAN/UBSAN passati. Sul primo candidato misurato test board, front H.265
  1920x1080@30, archive circa 15 fps e analysis circa 20 fps. Un TERM
  controllato al solo bridge il 24/09 alle 16:55:38 CEST ha prodotto
  `sdk_release=confirmed exit_status=0`, `capture_epoch_ended
  outcome=clean_exit` e nuova epoca ammessa alle 16:55:49 senza hold;
  il Tracker si è ricollegato. Restano soak del candidato finale con guard,
  verifica delle risorse/termiche, rilascio firmato e qualifica deployed board.

### Regressione scoperta nel candidato finale su test board (24/09, 17:32 CEST)

La seconda promozione misurata ha avviato front, archive e analysis e ha
superato la misura esatta (9.382 entry, 554.215.671 byte). Dopo un TERM
controllato al bridge, il rilascio dell'epoca 1 era confermato (`exit_status=0`).
Il successore PID 1093223 è però rimasto in `D`/`folio_wait_bit_common`, un
solo thread e `SIGTERM` non ancora gestito. Il budget di bootstrap di 5 s è
scaduto, poi l'arresto via TERM ha prodotto `signal: terminated`, stderr vuoto
e nessuna ricevuta SDK. Il nuovo guard ha quindi correttamente trattenuto
l'ammissione nel PAL di stato. Non attribuire il fault all'SDK: la diagnostica
è compatibile con page-in del loader, ma non ne dimostra da sola l'assenza di
risorse hardware inizializzate. Il precedente episodio analogo è documentato
in `2026-09-16-native-startup-quarantine-and-stop-signals.md`.

La correzione successiva in verifica **non estende il limite di avvio**: dopo
il suo fallimento, solo se il processo posseduto è bloccato in page-in prima
del gestore TERM, gli lascia fino a 30 s separati per raggiungere il gestore
prima di inviare TERM. Il worker tardivo non viene ammesso. Se non arriva una
ricevuta di rilascio, il guard resta attivo; nessuna deduzione automatica di
cleanup riuscito. Test unitario del percorso e suite Go broker/cmd/provider
passati; qualifica integrata test board ancora richiesta. deployed board non ha ricevuto
questa candidata.

Revisione della pulizia del nuovo publisher: il socket delegato riceve
`shutdown` **prima** del join limitato. Senza questa sequenza, un invio al
proprio deadline di 5 s poteva correre in parallelo con il join di 5 s e
trasformare un fault del solo output `analysis` in un rilascio dell'intera
sorgente non confermato. Suite native ASAN/UBSAN passata; resta da verificarlo
sul dispositivo insieme alla candidata completa.

test board è stato riavviato in modo controllato alle 17:40 CEST dopo misura esatta
PASS, assenza di bridge attivo e di transazioni correnti. Alle 17:51 CEST non
rispondeva ancora né su SSH/Tailscale né sull'indirizzo LAN d'inventario;
nessun guard è stato cancellato e nessuna nuova promozione è stata tentata.
Il ripristino del dispositivo e la verifica del nuovo boot precedono qualsiasi
rilascio di questa candidata. deployed board ha continuato a finalizzare MP4: il
segmento delle 17:40 CEST contiene HEVC 640x360 e audio Opus per 600,08 s,
verificato con ffprobe alle 17:51 CEST sulla release precedente.

### Ripristino test board e qualifica ARM64 (28/09)

La mancata registrazione dopo la riaccensione non era un nuovo fault della
camera: il guard dell'incidente `1092642-2`, scritto il 24 settembre, continuava
a trattenere l'ammissione. SHA-256 del guard:
`b11bd9f0d4d4f5a035ef6aed7a12ed4bbd697feb6b9c6252e068ea78bdcc5eb0`.
Il boot dell'incidente era `891288b0-229e-443f-9960-fe505bbbc4a5`; il boot
verificato dopo il power-cycle dichiarato dall'utente è
`ce10d046-ca26-4f56-b591-b7b8c860695b`.

Il nuovo tool standalone `tools/recover_capture_guard.py` non interpreta un
restart come reset hardware. Richiede amministratore, conferma del power-cycle
completo, boot differente, identità/hash esatti dell'incidente, UID di stato
atteso e proprietari della cattura fermi. Con lock di manutenzione e misura
esatta verificati, il guard è stato **archiviato**, non cancellato, insieme
alla ricevuta di reset sotto il PAL `frame-cleanup-recoveries/`. Il controller
ha riconfermato configurazioni identiche e misura PASS prima del riavvio.
Alle 12:59 CEST la release precedente ha ripreso front H.265 1920x1080@30,
archive circa 15 fps e analysis circa 20 fps, code/drop zero. Questa evidenza
dimostra ripristino operativo, non qualifica della candidata successiva.

I nuovi guard registrano anche il boot dell'incidente. I sei test del tool
passano sul workstation e sul Python 3.10 di test board; il parser gestisce i timestamp
RFC3339Nano scritti da Go. I test Go di broker/cmd/provider Linux e la
compilazione Go/native ARM64 passano. La suite nativa completa ASAN/UBSAN passa
anche su test board: due fixture V4L2 necessitavano trap esplicite per simboli encoder
trattenuti dall'instrumentazione ARM64, senza aprire codec/vendor reali.

Gli stop di prequalifica **non hanno attivato release fallite**: prima mancava
il percorso della cache Go nel processo isolato, poi mancavano simboli delle
fixture. La procedura è stata rivista: suite offline prima della manutenzione,
riuso dei binari solo con input e hash identici, un'unica promozione dopo PASS.
Job corrente: `/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r4`;
piano sorgenti SHA-256
`3c4b0edbb1124d3bca7cc8c19b16827baee11bb4fc9700370492cda387b2243f`.
La candidata è stata attivata, ma il **postflight reale è FALLITO** alle
13:55 CEST: l'encoder archive H.265 640×360, istanza VPU 1, ha restituito
`dequeue_output sdk_code=-268435443` (`HB_MEDIA_ERR_WAIT_TIMEOUT`, budget SDK
invariato di 1.000 ms). Il bridge si è chiuso e il Tracker è stato riavviato:
non è un falso negativo del test. Analysis aveva consegnato tutti i 1.096
frame, senza errori, drop o slot occupati. Il timeout VPU è quindi l'innesco
osservato; scheduling/I/O, leak o difetto hardware non sono ancora cause provate.

È emerso un difetto diagnostico distinto: `rc` conservava l'ultimo successo,
per cui la pulizia dichiarava `operation_status=0 exit_status=0` anche dopo
errore archive. La correzione nel checkout distingue completamento del ciclo
da uscita anticipata e conserva fase/esito del fault prima della pulizia.
Test dell'helper standalone PASS; qualifica nativa completa di questa ultima
variazione e recovery isolato dell'encoder ancora necessari. Nessun fault test
o soak della candidata r4 è stato avviato o dichiarato positivo.

Il rollback canonico ha ripristinato la base firmata pulita, non lo snapshot
development precedente con gli altri hotfix. Il tentativo di ripristino ha
scoperto un difetto del promoter: la pulizia delle release development non
referenziate cancellava anche la release usata come **sorgente del ripristino**
e lo strumento in esecuzione, prima di leggerne i moduli/payload. Lo snapshot
precedente, verificato prima dell'operazione, è stato rimosso da questa pulizia;
la candidata r4 conserva gli altri hotfix e rimane disponibile. È stata inoltre
copiata fuori dalla collection delle release, sotto
`/var/lib/nemesi/state/operator-backups/candidate-retained-20260928/snapshot`.
Corpus, configurazioni e registrazioni non sono stati cancellati. Il servizio
è stato riavviato sulla base pulita; non dichiarare il ripristino completo
degli hotfix attivo finché non viene verificato il successivo candidato.

La correzione del promoter protegge, durante entrambe le collection, le root
che contengono payload e strumenti in uso. Dodici test batch locali, compreso
rollback → ripromozione da una sorgente development inattiva, e il contratto
fault passano. Una futura promozione usa sorgenti private nel PAL, non una root
che il promoter precedente potrebbe eliminare. Non cambiare lo stato firmato
o il link a mano per aggirare la procedura.

Ulteriori protezioni nel checkout:

- Un errore delle operazioni SDK sui buffer avvelena la generazione della coda
  encoder: niente nuovi input, rate-control o IDR finché il destroy non conferma
  il rilascio. Questo impedisce l'accumulo e la falsa correlazione tra nuovi
  input e output vecchi anche mentre un rollback di branch è pendente. Un
  errore di copia dopo restituzione confermata del buffer non viene confuso con
  ownership SDK incerta. Regressioni H.264/H.265 per le quattro operazioni,
  dieci chiamate dopo ogni fault senza nuove chiamate SDK, poi destroy/reopen.
- La rimozione del guard pendente richiede lo stesso incident ID e
  `cleanup_pending`; guard terminali, diversi o invalidi restano intatti.
- Il record terminale generico `worker-operation` conserva la fase nel
  riepilogo dell'epoca. Il solo `encode_failed`, potenzialmente contenibile
  da una transazione di branch, non è trattato come morte della cattura.

Suite nativa completa locale ASAN/UBSAN PASS. Test broker Go race, suite nativa
ARM64 ASAN/UBSAN completa, dodici test del promoter e build dei tre bridge SDK
dell'ulteriore candidata sono **PASS** nel job privato
`/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r5`, delta
SHA-256 `675106e41289d0e78d9b6c630ba067a12c9bba0c801397060cbb231f802d389b`.
Anche la successiva build del binario Go aggiornato è PASS; evidenze separate
`prequal-pass.json` e `go-build-pass.json`, con hash degli input e degli output.
Queste prove offline non costituiscono attivazione, qualifica integrata o soak;
il recupero completamente isolato del branch archive resta da completare.
Questo aggiornamento documentale non richiede un'altra promozione.

La base pulita ha riprodotto il difetto originario alle 14:45:39 e 14:46:00
CEST: `output_send code=-5 peer_stage=io_deadline`, 1.000/1.001 ms,
49.020 byte inviati su 345.600, `action=retire_source`, con nuove epoche e
riavvii Tracker. È il guasto del trasporto analysis sincrono, **distinto** dal
timeout VPU archive osservato nella candidata r4; non attribuirli entrambi
all'encoder o a un leak senza una prova causale.

La promozione misurata r5, unit
`nemesi-capture-activate-20260928-r5.service`, manifest payload SHA-256
`06bbe1027b68a282644c6befec5fd8e674f4d5954382cd4751329174966b509f`:
47 target, 910 entry, 86.236.174 byte, è stata attivata ma ha **FALLITO** il
postflight alle 15:05 CEST, prima della cattura. Nella composizione manuale
del payload, la sostituzione dell'intera directory broker con l'albero di
sorgenti/build parziale ha omesso tre artefatti runtime:
`libvideo_broker_frame_peer.so`, `libgstvideobrokertone.so`, `video-ingest-gst`.
Il supervisore ha rifiutato correttamente la libreria autenticata mancante;
non è un rifiuto credenziale né un nuovo fault della camera. Tutti gli altri
hotfix sono presenti, ma questa candidata **non è un ripristino funzionale
completo**. Misura esatta PASS 9.385 entry/546.887.564 byte; la misura prova
integrità del pacchetto incompleto, non sufficienza funzionale. La sequenza
di qualifica si è fermata; nessun fault o soak r5 è stato eseguito. Servizio
fermato per evitare il ciclo di avvii falliti. Dati/config preservati.

Correttiva di confezionamento nel promoter: riusare il contratto centrale
`build_sandbox.py:REQUIRED_OUTPUT_FILES`, senza duplicare una lista broker.
Un hotfix non può rimuovere o rendere illeggibili/non eseguibili gli artefatti
obbligatori presenti nella base firmata, neppure se uno snapshot development
precedente li ha già persi. Controllo preventivo prima del clone (salvo build
nel candidato) e secondo controllo prima di misura/switch. Sedici test batch,
contratto batch e fault PASS locali, incluse directory incomplete rifiutate
prima della copia, ripristino dalla base firmata e permessi ridotti. Il
contratto caricato deve essere regolare, di proprietario fidato e non scrivibile
da gruppo/altri; le fixture non allentano questo controllo runtime.

Riparazione misurata r6 nel PAL
`/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r6`:
ricostruzione della libreria frame con gli input già testati r5, ripristino
degli altri due artefatti invariati dalla copia protetta, promoter e regressioni
aggiornati. Non ripetere le build Go/native già valide, non scrivere nella
root attiva e non fare rollback automatico alla base pulita già guasta.
La riparazione conserva il resto della root r5; postflight, fault, soak e
segmenti nuovi decodificabili restano necessari prima della qualifica.

La r6 è stata attivata con successo: manifest
`d0414a7728e334cf4d45e31b2ad78c2ac42f2d80e57773ed2f9304d5e18e1507`,
root `dev-1789244163760113403-d0414a7728e334cf-c539bc952283`, misura
9.388 entry/551.730.129 byte. Postflight reale PASS; MP4 A/V ripreso alle
15:30 CEST. Un SIGSTOP del Tracker di 4 s ha lasciato cattura/epoca invariati
e archive è avanzato da 4.133 a 4.223 scritture. TERM cooperativo alle 15:34:44:
SDK release confermato, successore pronto in circa 7 s, Tracker SDK in 21 s.
Questi esiti sono prove parziali, **non** fault-pass o soak-pass.

Alle 15:48 sono avvenuti ulteriori riavvii **non iniettati**: Tracker
`starts=4/exits=3` e cattura epoca 3. Il validatore della prova deliberata
consente solo il turnover esatto richiesto (`starts=2/exits=1`), senza
alterare il validatore di soak, e ha correttamente rifiutato questi eventi.
Il controller si è fermato prima di qualsiasi riavvio per baseline/soak.
Non azzerare la cronologia né ampliare le eccezioni per ottenere un PASS.

La traccia mostra un'attesa sull'**invio** archive di 2.959,60 ms, contro un
massimo di 7,25 ms nell'encoder hardware. Subito dopo, salto della sequenza
H.265 di 87 frame e ritiro della sorgente prima della pubblicazione; rilascio
SDK confermato e ripartenza canonica. Un precedente salto di 37 era stato
recuperato con nuovo IDR. Nello stesso intervallo la consegna analysis si
interrompe dopo 245.100/345.600 byte e anche audio-broker ha un timeout I/O.
La causa comune di scheduling/I/O è un'ipotesi da verificare, non un leak
provato; non attribuire questo episodio al watchdog no-frame di 30 s.
Nessuna variazione di timeout, soglie termiche, FPS o fallback software.

### Contenimento della diagnostica e pressione I/O (28/09)

La scrittura sincrona dei log sotto il mutex del broker è un percorso di
backpressure confermato da una regressione con sink bloccato. La candidata
Go r7 compone un writer standalone a una sola goroutine, coda di 256 record
immutabili (massimo 32 KiB ciascuno), scarto contabilizzato senza retry nel
ciclo media e flush di chiusura limitato. Quando il sink torna disponibile
emette `diagnostic_backpressure` con il numero di record persi. Questo protegge
media e control-plane dal **destinatario dei log**, non risolve per definizione
tutte le attese del kernel o della SD. Le ricevute native e i guard continuano
a essere elaborati indipendentemente dall'esito di pubblicazione del log.
Sei regressioni del writer più il percorso reale dei metric log/mutex PASS con
race detector; suite cmd/broker/diagnosticlog completa PASS sulla workstation.

L'osservatore privato read-only su test board, distinto dal soak, ha raccolto
1.753 campioni tra 16:10:51 e 16:25:27 CEST: 4.809 major page fault di sistema,
nessun nuovo swap-in/out, attese `folio_wait_bit_common`, `jbd2_log_wait_commit`
e `do_get_write_access` in più processi, ritardi del proprio campionamento fino
a 2,776 s. `hash_sendmsg` e `osal_completion_wait` del bridge indicano invece
attese degli acceleratori: non classificarle automaticamente come attese SD.
Parte della finestra comprende prove/build concorrenti a bassa priorità;
non estrapolarla come misura certificata del solo carico operativo normale.
Il recorder ha finalizzato un nuovo MP4 alle 16:10, con sync massimo 4.173 ms.

Due suite ARM64 con fixture su SD sono fallite senza modificare i test:
`TestSourceSupervisorRestartWaitsForRealEndpointTeardown` e successivamente
`TestVideoBrokerDetectsUVCStreamOnFailure`, il cui writer filesystem ha impiegato
4.388 ms oltre il limite di 2 s. Evidenze preservate nel PAL r7:
`go-race.log`, `go-race-r2.log`. Dopo questi due tentativi si è cambiato metodo,
non aumentato il limite: regressioni software/ownership con fixture temporanee
RAM, **separate** dalla qualifica integrata su SD. Un PASS software in RAM non
qualifica la SD e non cancella i due fallimenti. Il soak reale resta invariato,
almeno 2.700 s (o finestra recorder più lunga), input/profilo/SD reali,
collezione autenticata e Tracker medio almeno 18 fps. La proposta r7 non è
ancora una release ufficiale firmata e non chiude l'issue architetturale.

**Limite architetturale esplicito:** il fault del solo output analysis non
abbatte subito camera/archive. Se il trasporto è definitivamente ritirato,
il recovery attuale può ancora usare il teardown verificato dell'intera
sorgente dopo il watchdog no-frame; non dichiarare una riconnessione del
canale senza gap archive o una completa indipendenza dei riavvii senza la
relativa prova. La matrice di accettazione sotto resta aperta.

deployed board non ha ricevuto questa candidata. Nel controllo del 28 settembre SSH
va in timeout e Tailscale riporta ultima presenza il 25 settembre: lo stato
della registrazione corrente è **non verificato**, non inferito dal PASS del 24.

### Cascata di cancellazione individuata e contenuta (28/09, 16:29 CEST)

Sulla candidata r6, epoca 3, il mittente analysis ha raggiunto il suo limite
di 3 s dopo un invio parziale (`49020/345600` byte). Il native ha correttamente
ritirato **soltanto** l'output. Go invece ha ricevuto
`source_receive_deadline`, `cause=local_deadline`, `child_context_done=false`
e ha cancellato la cattura: il contenimento copriva soltanto `ErrUnavailable`,
non il deadline locale. Il successivo `stop_signal=15`, fase SDK
`capture_dequeue`, era un effetto di quella cancellazione, **non prova di un
nuovo guasto primario del dequeue**. Il rilascio SDK è stato confermato e
l'epoca 4 è stata ammessa alle 16:29:16. Evidenza originale: journal della
unit, invocation `19a7e273b4f849139dd457b9f2d9ebd8`.

La candidata r8 aggiunge classificazione tipizzata per fase dell'errore:
deadline/unavailable nel solo receive ritirano il canale mantenendo il pump
indipendente e la proprietà della cattura. Cancellazione del proprietario,
sender/binding ritirato o non autorizzato, record/integrità invalidi e guasti
della pubblicazione rimangono bloccanti. Non si riutilizza un canale parziale,
non si pubblicano pixel incompleti e non si aumenta alcun timeout. Un test
con worker nativo ASAN/UBSAN e mailbox reale verifica che un deadline iniettato
non interrompa il pump; il test del receiver verifica zero pubblicazioni e
zero retry dopo un trasferimento parziale. Suite host `go test -race` di
frameingress/cmd/broker/diagnosticlog PASS. Questa prova non sostituisce quella
onboard e non dimostra la scomparsa delle attese I/O.

Il primo tentativo software in RAM r7 è **fallito**: il lungo prefisso
temporaneo superava la capacità del pathname Unix (`bind: invalid argument`),
impediva l'avvio delle fixture e provocava anche un nil dereference nel test
di routing dopo il mancato bind. Non attribuire questi fallimenti alla SD o
alla correttiva del logger. Log completo nel PAL r7,
`go-race-ramfixtures.log`; nessuna promozione r7 è avvenuta. Il nuovo controller
r8 usa un root RAM corto ed esclusivo `/run/v8`, con le stesse prove e deadline.

**Limite della candidata intermedia r8, non installata:** il watchdog analysis a 30 s usa ancora
il teardown della sorgente per ristabilire il canale. La classificazione r8
elimina la cancellazione immediata, ma non equivale a un rebind autenticato del
solo output. Finché questa condizione resta, non dichiarare soddisfatta la
riga della matrice «analysis recuperabile senza restart della camera» né
chiudere l'issue, anche se il soak risultasse positivo. deployed board non è stato
aggiornato; l'ultimo tentativo TCP/22 del 28/09 è scaduto prima dell'autenticazione.

### Correttiva strutturale: consegna analysis tramite snapshot RAM (28/09)

Non è stata promossa la correzione intermedia r8. Sulla macchina ARM64 le
prime tre suite Go sono passate, ma il controller complessivo è scaduto a
360 s prima di una prova completa del quarto pacchetto; nessun binary/payload
qualificato né `prequal-pass.json`. Il codice intermedio di classificazione
del trasporto è stato sostituito nel percorso runtime, non mantenuto come
fallback. Le evidenze r8 rimangono nel relativo PAL.

La candidata completa r9 elimina **il socket di pixel per-frame** tra il
worker nativo e il broker. La consegna analysis usa un secondo mailbox memfd
a due slot atomici, separato da quello dei frame nativi. Namespace, generazione,
capacità e FD sono ammessi prima di allocare; entrambi i mailbox sono ereditati
dal solo worker autenticato, sigillati e confermati nel bootstrap. Nessun FD
per frame, ACK, coda crescente, ricampionamento dei pixel nel broker o reader
software alternativo. Il provider nativo pubblica i pixel già prodotti
dall'acceleratore e la loro esatta identità di cattura/trasformazione.

Il nuovo receiver generico copia snapshot completi in due buffer privati
ammessi, ordina le due pubblicazioni per sequenza di output e verifica SHA
prima di emettere un riferimento. Un lettore lento perde snapshot intermedi;
non può lasciare un record parziale da risincronizzare. Deadline/capacità/
retention consumano il singolo snapshot senza retry e consentono il prossimo
poll sullo stesso worker/epoca. Errori di identità, epoca, layout, integrità o
cause strutturali combinate con un deadline rimangono fatali. Il pump dei
frame nativi resta indipendente; archive non attende il receiver analysis.

Il publisher verifica la scadenza prima di iniziare il commit RAM atomico:
non lascia uno slot a metà soltanto perché lo scheduler lo sospende dopo
l'inizio della copia. Autorità e stop restano verificati prima/dopo il commit.
Le nuove risorse RAM vengono contabilizzate separatamente; i limiti e i
profili centrali non sono stati cambiati. Il watchdog continua a rilevare un
guasto vero della produzione analysis; non serve più per riparare una sessione
di pixel resa inutilizzabile dalla sola lentezza del reader.

Evidenze software host per questa candidata:

- `go test -race -tags nemesi_integration` su frameingress/cmd/broker/logger:
  PASS, inclusi 9 percorsi codec/geometria con decoder sintetico, crop nativo
  esatto e verifica Python indipendente. Le rappresentazioni chunk della
  prova Python sono costruite **solo nel test** dagli snapshot RAM originali;
  non sono il trasporto runtime e non provano accelerazione fisica.
- Suite nativa completa ASAN/UBSAN: PASS. Il test mailbox pubblica 20 frame
  con consumer assente, conserva esattamente gli ultimi due e non ritira la
  cattura; argv reali verificano il nuovo bootstrap.
- Test integrato con worker nativo reale e consumer oltre il proprio deadline:
  successivo output e pump recuperano sulla stessa cattura/epoca. L'oracolo
  separato di due crop esatti usa una barriera **solo della fixture** per non
  confondere sostituzione/backpressure ammessa con errore geometrico; conserva
  tutte le verifiche pixel, identità, budget e sanitizzatori.
- Regressioni di ordinamento, replay, corruzione, epoca estranea, cancellazione
  e errore misto autorità+deadline: PASS. Un primo timeout di fixture con
  attesa di tutti i frame, incompatibile con la sostituzione prevista dei
  mailbox, è conservato; non è stato trasformato in qualifica runtime.

Originali privati host in
`<private-workspace>`, log
`mailbox-complete-host-race.log`, `mailbox-integrated-host.log`,
`native-output-mailbox-host-r2.log`. Candidata/source/prove ARM64 nel PAL
`/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r9`.
**Non ancora installata o qualificata:** richiede prove ARM64, promozione
misurata, fault integrati, soak su SD invariato e nuovi segmenti A/V
decodificabili. Nessun rilascio su deployed board e nessuna chiusura dell'issue.

### Prequalifica r9 respinta, fixture corrette nella r10 (28/09)

La r9 non è stata attivata: il gate ARM64 ha respinto la suite, e il
controller di promozione si è fermato senza `prequal-pass.json`. La r6 è
rimasta attiva e ha finalizzato alle 17:00 CEST un MP4 nuovo con 8.999 frame
video e 30.000 audio. Questo non cancella i precedenti guasti della r6.

Le due segnalazioni race individuate appartengono alla diagnostica della
fixture Python: `bytes.Buffer.String` leggeva mentre `exec.Cmd` scriveva.
Il buffer di log del test è ora sincronizzato. Il timeout del peer iniziava
anche prima degli import Python/OpenCV: un handshake **solo della fixture**
attende la preparazione del processo entro il contesto totale già previsto,
poi avvia le operazioni native con i limiti di un secondo invariati.

La prova del consumer lento verifica ora esplicitamente un output **nuovo**,
con origine successiva, sulla stessa cattura/epoca dopo il proprio deadline.
Non pretende la conservazione di tutti i frame della sorgente non cadenzata.
Gli oracoli separati pixel-esatti sincronizzano lo staging della superficie
prima della pubblicazione: una contesa del lock Store, per la quale il drop
è ammesso, non deve rendere impossibile la prova di due soli frame finiti.
Le barriere non esistono nel runtime né nella camera fisica; nessuna soglia
di qualità/FPS o controllo pixel è stato rimosso.

Host: i nove casi decoder/ROI/Python, il worker nativo con consumer lento e
il percorso daemon → Tracker con switch front/rear/front passano insieme
con race detector (`mailbox-readiness-host-race-r2.log`). L'envelope di suite
ARM64 è cinque minuti per **intero pacchetto**, anziché i 120 s introdotti
dal controller r9; include molte compilazioni ASAN. Non cambia i deadline
di avvio, frame, richiesta, switch o recovery. I PASS ARM64 r9 dei pacchetti
broker/logger vengono riusati solo dopo parità esatta dei relativi sorgenti;
i pacchetti modificati vengono eseguiti nuovamente per intero.

Candidata r10 e controller indipendente nel PAL
`/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r10`.
Archivio input SHA-256
`c7aad026fd9070cf7839dc843eb794fec9f889abec50062e9114679c5ee65d7f`;
controller SHA-256
`e7cd74be0df3f6285573970d1bff56d8284037bfb47d3c2e65e21aeabaaf0427`.
Sequenza fail-closed: suite ARM64/native/build → misura/promozione completa
→ fault isolati → baseline dopo i soli fault pianificati → soak su SD
invariato. Un fallimento ferma la sequenza, senza cancellare guard o prove.
**Stato al dispatch: prequalifica in corso, non installata, nessun PASS soak.**

La r10 è stata a sua volta respinta prima dell'attivazione. Le prove native
di cattura/restart e consumer lento hanno avanzato senza guasti del mailbox;
il gate complessivo è fallito per preparazione incompleta della composizione
di test. Mancava `task_frame_tracker_fixture.py` con le dipendenze Python;
il percorso `/run/v10/<nome-test><random>/002/routing-control.sock` arrivava
a 109 byte, oltre i 107 utilizzabili del socket Unix. Inoltre la libreria
peer veniva compilata dopo l'avvio del broker di test, consumando la sua
attesa di grant. Il budget di compilazione del peer condivideva erroneamente
quello già consumato dal decoder ASAN. Le evidenze r10 restano intatte.

Nella r11: root temporaneo corto `/run/r`; launcher di test esplicito;
sorgenti Python copiati dalla release **installata**, con hash registrati e
verificati prima della promozione, senza modelli/ambienti/dati né cambiamenti
al payload Tracker/Registry; libreria pronta prima di avviare il broker;
45 s di preparazione distinti per ciascuna invocazione compiler, non per le
operazioni media. Suite completa host Go race/integration nuovamente avviata.
La sequenza di promozione/fault/soak rimane la stessa e non ammette alcun
fallimento software come PASS. Questo supera i difetti della procedura,
non certifica a priori il funzionamento della candidata sul dispositivo.

Job r11: `/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r11`,
unit `nemesi-capture-prequal-20260928-r11.service` e
`nemesi-capture-chain-20260928-r11.service`.
Input SHA-256 `bdd151a3743982ecb85ef1412bb98d8539e585a25b767d47537f155048cb9233`;
controller SHA-256 `20d936ef031d0745aae6b7afb14f67b6f6268a5ecbd1eed4e44917721c2b53e9`.
**Stato al dispatch r11: release r6 ancora attiva; nessuna nuova attivazione
o qualifica dichiarata.** Non avviare controller paralleli al job corrente.

La r11 è stata respinta senza attivazione. Il decoder nativo ha completato
le superfici, ma la preparazione Python consumava la finestra della fixture;
il broker separato iniziava l'attesa di connessione prima degli import del
Tracker. Profilazione isolata: import OpenCV 7,693 s, tutti gli import 8,289 s;
fixture con `OPENBLAS_NUM_THREADS=1`/`OMP_NUM_THREADS=1`: OpenCV 3,682 s,
totale 4,249 s. Questi valori non sono misure del percorso media né del
servizio installato. Una race residua del solo log di test era dovuta al
metodo `ReadFrom` ereditato da `bytes.Buffer`, usato da `io.Copy` senza `Write`:
buffer ora non embedded, con regressione esplicita contro quel bypass.

La r12 aggiunge il gate di preparazione del Tracker nel test del broker
separato: import e compilazione precedono le operazioni di protocollo, i cui
deadline restano invariati. I thread numerici limitati appartengono soltanto
alle fixture software, senza lavoro BLAS, e **non** all'ambiente della release
o del soak. Suite host race completa di frameingress/cmd PASS nella prova
`mailbox-preparation-final-host.log`; `go vet` PASS. Nessun errore ARM64 è
stato riclassificato come successo e le prove precedenti restano conservate.

Job corrente r12: `/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r12`,
unit `nemesi-capture-prequal-20260928-r12.service` e
`nemesi-capture-chain-20260928-r12.service`.
Input SHA-256 `146da3236b675941fd92a49cb645a8833e4fb4902344d0b8e1f45887ea2ba899`;
controller SHA-256 `f8974a0e5d92a257afa20a393de92eceaf65240da7c5b21fc4d77eb1bca8371e`.
Al dispatch r6 rimane attiva. deployed board, nuovamente verificato il 28/09 alle
17:38 circa, va in timeout TCP/22 prima dell'autenticazione: niente deploy
o conclusioni sul suo stato di registrazione corrente.

La r12 non è stata promossa: restava un envelope aggregato della fixture che
includeva uscita ASAN e preparazione di due processi Python indipendenti;
lo stesso difetto accumulava tre uscite ASAN nel test dei tre switch. Nella
r13 il trasporto nativo mantiene la propria finestra di 10 s, ogni consumer
indipendente mantiene 10 s e ogni ciclo di switch mantiene 20 s; i deadline
media/peer restano quelli già indicati. Il servizio della fixture attraversa
i tre cicli con lifetime distinto, senza trasformarlo in un limite media.
Ogni ciclo richiede ora anche `closed` dal nativo entro i propri 20 s;
il successivo `Wait` del sanitizzatore resta verificato separatamente.
Nessuna prova pixel, autenticazione, rilascio o FPS è stata eliminata.
Host race frameingress/cmd PASS (`mailbox-independent-phase-host.log`), vet PASS.

Nel frattempo **la r6 attiva ha riprodotto il fault causale** alle 17:38:13:
analysis `output_send`, 49.020/345.600 byte, 3.002 ms, poi cancellazione della
cattura. Rilascio SDK confermato alle 17:38:17, epoca successiva ammessa;
il recorder ha finalizzato alle 17:40:10 un MP4 con 8.831 frame video e
30.000 audio. Queste evidenze provano ripresa, non stabilità della r6, e non
sono state attribuite alla nuova candidata mai attivata.

Dispatch r13: PAL `capture-structural-20260928-r13` nello stesso percorso,
unit `nemesi-capture-prequal-20260928-r13.service` e
`nemesi-capture-chain-20260928-r13.service`.
Input SHA-256 `ea350dcadb476c3652e70d1ee1a41840ddb3d62f3872a10d33a65521caa31728`;
controller SHA-256 `2248d030a9d64bf104c02145aec262c4ff262d545d68e716203e355ab3b4835f`.
Il runtime RAM/logging della candidata è invariato dalle ultime iterazioni
sulle fixture. Attivazione, fault, soak reale e qualifica cliente rimangono
gate aperti finché non esistono le rispettive prove positive.

### Qualifica ARM64 r13/r14: completamento funzionale distinto dall'uscita

La r13 non è stata attivata. Il package ARM64 `cmd/video-broker` ha superato
la suite race/integrata in 145,497 s; `frameingress` ha fallito in 247,515 s
per un caso 1920x1080/H.265. Il consumer aveva stampato l'esito positivo,
ma è stato ucciso durante l'uscita dell'interprete: un messaggio di log non
costituisce prova di completamento. Nessun fallimento è stato ignorato.

La r14 mantiene invariato il runtime della candidata. La fixture richiede
ora una conferma esplicita su FD dopo tutte le verifiche pixel/provenienza
e release, entro la stessa finestra funzionale di 10 s. Il processo Python
ha un lifetime separato di 30 s e deve comunque terminare con exit 0;
le chiusure Go hanno contesti propri di 1 s, senza riutilizzare il contesto
funzionale scaduto. Host race `frameingress` PASS 18,401 s, cmd PASS cached,
vet PASS (`mailbox-functional-ack-host.log` nel workspace privato persistente).

Dispatch 28/09 circa 18:22 CEST: PAL
`/var/lib/nemesi/state/operator-backups/capture-structural-20260928-r14`,
unit `nemesi-capture-prequal-20260928-r14.service` e
`nemesi-capture-chain-20260928-r14.service`.
Input SHA-256 `6b96880590048e9ae3522b6c37d5ae1f920341c728a8139b5d42e5c44b1f8181`;
controller SHA-256 `4ba97550cda4c3454f027922d689c36e2e49bae770225c10a7bfd9bf7acc503b`.
La suite ingress ARM64 è ripetuta integralmente; la prova cmd r13 è riusabile
solo dopo confronto esatto del package e di tutte le dipendenze runtime
Go/C/header/SDK Python. Seguono sanitizzatori nativi, build, una promozione
misurata, fault controllati e soak reale di almeno 2.700 s (o finestra
recorder più lunga). La catena si arresta al primo gate non superato.

Al dispatch la r6 è ancora attiva: questi job non provano installazione,
funzionamento della nuova pipeline o qualifica deployed board. Restano necessarie
verifica di un nuovo MP4 chiuso/decodificabile dopo il soak, misura finale,
verifica della riconciliazione delle fixture nel checkout e qualifica specifica
MJPG/JPU di deployed board. Nessun dato, profilo o soglia termica è stato cambiato.

### Revisione del comparatore dopo r14 (28/09)

La r14 non è stata promossa: tutti i casi dei consumer hanno superato la
conferma esplicita, ma la parità Python C/HD 1920x1080/H.264 è stata uccisa
dal proprio limite di 10 s. Package ingress FAIL 258,912 s, decoder 248,53 s.
Questa prova costruiva milioni di byte con generatori Python per ogni piano.
Invece di aumentare ancora un limite, la r15 costruisce lo **stesso** pattern
modulare con righe limitate e `bytes.translate`: confronto di tutti i pixel,
stessi assert di origine/identità/layout/hash, stessa finestra di 10 s.
Include un controllo indipendente della formula originale per entrambe le
scale con wrap modulo 256. La verifica locale aggiuntiva su tutte le sei
combinazioni risoluzione/scala produce byte identici; singola costruzione
1080p circa 0,0096 s sull'host, non una misura della pipeline test board.
Host ingress race PASS 22,290 s, vet PASS (`mailbox-oracle-final-host.log`).

La revisione è solo dell'oracolo di test: runtime RAM invariato, nessun nuovo
deploy per i candidati respinti. Dispatch r15: PAL `capture-structural-20260928-r15`,
unit `nemesi-capture-prequal-20260928-r15.service` e
`nemesi-capture-chain-20260928-r15.service`.
Input SHA-256 `d2b2510f94b33907cd109b06ca20f6134496ef66f23fcacf7fe82fd7fe1b1a65`;
controller SHA-256 `c7380b51be3748f265d66eef2a89c632af89bfab5333ac944cf6813120220945`.
Restano aperti gli stessi gate di attivazione/fault/soak/MP4 e deployed board; r6
ancora attiva al dispatch. Le prove fallite r13/r14 sono conservate nel PAL.

La fixture root `src/task_frame_tracker_fixture.py` è già tracciata in Git
(commit `9a9c1657`) ed è identica alla copia usata dai controller:
SHA-256 `6d186a4eeb2a2e568c9715cd6c38fed65db4c04a2049348e14429d6b4219c211`.
Non è lavoro unico rimasto soltanto nel temporaneo. Alle 18:31 il recorder
della r6 ha prodotto MP4 chiusi alle 18:10/18:20/18:30; non è evidenza della
nuova candidata né, senza decode, prova di integrità video. deployed board TCP/22
nuovamente in timeout, prima dell'autenticazione.

### Esito r15 e checkpoint prima della ripresa (28/09)

Prequalifica r15 completa PASS: ingress ARM64 race/integrato 227,596 s,
prove cmd/broker/logger riutilizzate soltanto dopo confronto degli hash,
regressioni native ASAN/UBSAN, sei test del validatore fault, tre bridge e
build Go. La candidata non è stata attivata: `runtime_quiesce_guard.py`
ha respinto l'identificativo testuale `test-board-capture-mailbox-20260928-r15`;
il contratto richiede un identificativo SHA-256 esadecimale di 64 caratteri.
L'errore è del controller operativo e si è verificato **prima** del drain,
dello stop del servizio e della promozione. La misura della r6 resta PASS
(9.388 entry / 551.730.129 byte). Log conservato nel PAL r15:
`chain-activation.log`; nessun fault/soak r15 avviato.

Alle 19:58 test board è ancora sulla r6 e finalizza nuovi MP4 alle 19:40/19:50.
Non sono prove della candidata e non sostituiscono decode/soak. deployed board è
spento per indicazione dell'utente e resta escluso da accessi/deploy finché
non torna disponibile. Il checkpoint Git richiesto conserva tutto il lavoro
senza dichiarare chiusa l'issue o modificare i criteri; la ripresa deve
correggere il formato del controller e riutilizzare la prequalifica valida,
non ricompilare o ripetere l'intera suite per un identificativo operativo.

### Ripresa r16: contratto controller corretto e prove riutilizzate

Checkpoint richiesto committato e pubblicato su `origin/main`:
`0b1b737c46d9ffe693c6a643cdf989c2f7a95ee5`, 57 file, verifica indipendente
del ref remoto PASS e checkout pulito prima della nuova correzione.
Suite host checkpoint race frameingress 16,559 s/cmd 14,006 s,
broker/logger PASS cached; promoter 16 test PASS, validatore fault 6 PASS.

La correzione operativa r16 mantiene la candidata r15 byte-identica.
Il confronto dell'archivio r15 con `src/` al commit indicato è PASS.
I due identificativi quiesce/drain (attivazione e baseline post-fault) sono
ora SHA-256 della fase e dell'artifact, non etichette testuali. Due test
statici/isolati verificano entrambi i controller e il rifiuto di input
non validi, senza eseguire operazioni live. Sorgenti del runner conservate
in `src/setup/tests/capture_recovery_20260928/`: non sono un componente
runtime o un rilascio firmato. Prove r15 e payload vengono riutilizzati
soltanto dopo verifica dei log, dei file compilati e dei consumer installati.

Controller r16 SHA-256
`d31c4e9ce54e0757641a09b1119706aefe54af499c704307e3432e752374b225`;
PAL previsto `capture-structural-20260928-r16`, unit
`nemesi-capture-chain-20260928-r16.service`. Nessuna nuova build,
riduzione di soglie, cancellazione di guard/dati o prova su deployed board.
Fino all'esito dei report postflight/fault/soak resta aperta la qualifica
fisica della pipeline RAM. Il goal e l'issue non sono chiusi.

### Attivazione r16 PASS; fault e soak restano aperti

La r16 è effettivamente installata sul root
`dev-1789244163760113403-fce129bd1a23692b-4912f0f42484`, manifest
`fce129bd1a23692bdd1861451557f55469cf5e13dca63c63605ff8c7bef13849`.
Copia/promozione misurata, configurazioni, artifact obbligatori e postflight
PASS. Misura indipendente delle 20:27: 9.395 entry / 551.756.611 byte PASS.
La copia ha richiesto diversi minuti su SD, con contatori di scrittura in
avanzamento; non è stata interrotta né sono state ridotte le verifiche.

La fase fault è fallita alle 20:18:33, non il deploy. Il test ha sospeso
**tutto** il Tracker per 4 s, compresa una inferenza BPU già in corso;
deadline SDK BPU 3 s. Il Tracker ha dichiarato
`BPU_FATAL operation=wait status=-6000008 action=process_fail_stop
reason=uncertain_task_lifetime`, è uscito con status 70 alle 20:18:01 e ha
ricevuto una nuova grant alle 20:18:03. La cattura PID 983529 è rimasta
nell'epoca 1: il broker non ha riavviato camera/decoder/archive per quel
fault del consumer. Il gate che richiedeva stesso PID/zero turnover ha
comunque respinto la prova. Non è una prova isolata di pressione analysis
e non deve diventare un'eccezione retroattiva o un deadline BPU esteso.
Sorgente e report della prova restano conservati; niente ulteriori SIGSTOP
di un processo con lavoro hardware per ripetere quel test.

Il campione iniziale era inoltre troppo presto per provare persistenza
archive: `archive_video_writes_total=0`, recorder in attesa della finestra
segmento dopo lo stop prolungato. Nuovo archive avviato alle 20:20, file
temporaneo di circa 34 MB alle 20:26: ripresa, non qualifica del file chiuso.
TERM nativo pianificato, baseline e soak r16 **non sono stati eseguiti**;
la catena si è fermata, mentre il servizio rimane attivo sulla nuova versione.

Cadence reale circa 12,96 FPS sia prima del deploy (r6, 20:03/20:04) sia
dopo (r16, 20:20/20:22). Attesa camera circa 70 ms, lavoro frame circa 7 ms,
decode circa 1 ms, zero timeout/corruzioni camera nelle finestre osservate.
Il dispositivo dichiara comunque 30 FPS negoziati; `auto_exposure=3`
(Aperture Priority), gain 15, white balance automatico. Origine esatta della
cadence ridotta **non provata**: non attribuirla automaticamente alla stanza
buia, alla CPU o alla nuova pipeline. Nessun controllo camera modificato.
Al momento di questa prova il gate Tracker >=18 FPS e il requisito fisico
30 FPS restavano aperti. La successiva decisione utente sui 13 FPS notturni,
documentata sotto, modifica solo l'accettazione del carico notturno test board,
non l'esito storico della prova o il requisito diurno.

Prossimo lavoro circoscritto: isolare correttamente il fault analysis senza
fermare la BPU, iniziare con archive realmente in scrittura, poi provare
rilascio/nuova epoca e soak con sorgente alla cadence richiesta; verificare
nuovo MP4 chiuso/decode. deployed board resta spento/escluso. Niente nuovo deploy
per questi aggiornamenti documentali; software e dati installati preservati.

### Nuovo segmento r16 verificato senza conversione di cadenza

Il segmento interamente successivo all'attivazione `20260928-182000.mp4`
è chiuso: 52.370.548 byte, durata formato 600,101844 s, video HEVC 640x360
7.764 frame (599,227722 s), Opus 29.963 frame (599,264500 s).
`ffprobe` PASS. Decode completo video/audio con FFmpeg, un thread, timeout
180 s: exit 0 senza errori riportati. Verifica esplicita successiva con
`-vsync 0 -progress pipe:1`: exit 0 alle 20:37:08, `frame=7764`,
`dup_frames=0`, `drop_frames=0`, `progress=end`. Il log è conservato nel PAL
r16 `decode-passthrough.log`. Non è un fallback software del servizio:
è soltanto una verifica offline di un MP4 chiuso, fuori dal soak.

La sorgente/cattura era ancora PID 983529/epoca 1 alle 20:33, senza nuovo
epoch-end nelle finestre osservate; il servizio è attivo sulla r16.
Queste prove confermano registrazione e decodificabilità sulla nuova
versione, non il requisito fisico 30 FPS, il test fault rifiutato, la
stabilità di lungo periodo o il goal generale. La catena r16 è terminata
FAILED al fault e non esiste un soak r16 ancora in corso. deployed board resta escluso.

### Decisione utente: circa 13 FPS accettabili di notte su test board

Il 28 settembre l'utente autorizza esplicitamente circa 13 FPS di notte.
Questa cadenza non deve quindi bloccare da sola un nuovo soak notturno;
non si applica il minimo Tracker >=18 FPS quando la sorgente notturna
fornisce circa 13 FPS. Dichiarare il carico notturno prima della prova,
misurare separatamente frame disponibili, avanzamento dei consumer,
scarti e latenza: l'accettazione non copre stalli, accumuli o perdite
aggiuntive introdotte dalla pipeline. Il solo numero FPS o l'orario non
dimostrano che la causa sia la scarsa luce o l'autoesposizione.

Restano invariati il profilo centrale, i controlli camera automatici,
gli acceleratori obbligatori, i limiti termici e i criteri di integrità,
recupero, persistenza e decodificabilità. Il requisito diurno di sorgente
30 FPS e Tracker circa 20 FPS resta aperto e richiede prova pertinente;
nessuna estensione automatica dell'eccezione a deployed board o ad altri profili.

Il fault r16 resta FAILED e il soak non è stato avviato: la nuova decisione
non cambia retroattivamente i report. Il runner storico con SIGSTOP globale
non va riutilizzato né corretto per trasformare quella prova in PASS.
Prima del prossimo soak resta da qualificare l'isolamento del solo consumer
analysis senza sospendere la BPU. Questo aggiornamento è documentale:
nessun deploy, restart, modifica delle soglie o dei dati.

### Ripresa dei test notturni: r17–r20 e limiti conservati

Controllo live del 28 settembre: release/manifest r16 invariati, misura esatta
9.395 entry / 551.756.611 byte PASS. Una nuova epoca della sorgente era già
attiva: alle 20:47:02 la prima aveva dichiarato
`interframe_resync_timeout:h265`, exit64, `sdk_release=confirmed`; il Tracker
aveva ritirato il binding ed era ripartito. Alle 23:22 sorgente epoca2,
circa12,96FPS, code vuote e temperatura Tracker circa78°C. Questo evento non
è prova di assenza di guasti della camera; conservare il limite, senza
attribuirlo alla pressione analysis o alla scarsa luce per supposizione.

r17/r18 si sono fermate al postflight per un errore del nuovo controller:
secondo start nel finally dopo avvio già riuscito, che riattivava il resolver
oneshot mentre il Tracker partiva. La traccia r18 prova config nuovo inode
root:root0640 per circa2,65s, quindi EACCES del Tracker e turnover. Non è un
fallimento della cadenza13FPS né BPU. Controller corretto senza deploy della
release; regressione protegge il singolo start. Il rischio generale di
metadata pubblicati prima del chown resta nella
[issue setup aperta](../../setup/issues/2026-09-28-profile-resolver-publishes-unreadable-config-before-chown.md).

r19: baseline/postflight PASS, Tracker senza turnover e archive già in
scrittura. Fault diagnostico FAILED: socket packet NV12 autenticato ma nessun
frame, timeout2s prima di applicare la pressione. Native FAP pubblica in RAM,
non nel mux packet; la
[issue endpoint aperta](2026-09-28-causal-analysis-packet-endpoint-connected-without-frames.md)
e la README conservano questo limite. Non riclassificare il fault PASS.

r20 riusa solo la baseline r19 ancora applicabile: stessi manifest, collector,
quattro hash config, invocation e PID/epoca sorgente, più campione canonico
autenticato fresco. TERM cooperativo del bridge tramite pidfd: **recovery
PASS in11,97355274099391s**, nuova epoca/child, Tracker con l'unico turnover
predeclared exit1, archive nuovamente in avanzamento, nessun cleanup guard.
Segue baseline distinta tramite quiesce/drain, singolo start e postflight PASS.

**Soak notturno r20 avviato circa23:56 CEST:**2700s, campioni0,5s, collector
installato invariato e zero eccezioni di turnover nel monitor continuo.
Temperatura iniziale79,148°C; recorder appena ripartito in attesa della
finestra segmento (archive_writes0 iniziali), non ancora prova di persistenza
nuova. La sorgente è PID1033119/epoca1 dopo il restart pianificato. Nessun
PASS soak al momento di questo aggiornamento; servono esito completo,
misura/config finali e nuovo MP4 chiuso/decodificato.

Controller corrente `qualify_night.py` SHA256
`b5b1f1c474431f281ff860ad4b2e143c687dcbb0190abb7b05debdd9a32cecc6`;
PAL `/var/lib/nemesi/state/operator-backups/capture-night-20260928-r20/`,
unit `nemesi-capture-night-20260928-r20.service`. Sorgenti/test in Git,
regressioni notte6 e identità controller2 PASS, contratto helper privilegiato
PASS. Queste prove non sostituiscono la qualifica fisica SDK sotto pressione.
Monitor di esito app `qualifica-notturna-test-board` ogni5min, quiet se invariato:
verifica finale/fallimento e stop del monitor, niente nuovi deploy o accessi
deployed board. Non chiude automaticamente l'issue o il goal generale.

**Esito successivo r20: FAILED**, 29 settembre00:00:49,191569 CEST
(`2026-09-28T22:00:49.191569+00:00` nel report): fase
`night-continuous-soak`, componente `supervisor`, sottofase
`authenticated_snapshot`, durata4002ms. Nessun PASS del soak2700s;
`chain-failure.json` e journal restano nel PAL originale. Il monitor app è
stato sospeso dopo il fallimento terminale, non il goal. Non sono stati
allargati timeout, ridotte soglie, cancellati guard o ritentati fault/restart.

Diagnostica successiva senza restart: la pipeline pubblica circa12,95–12,96FPS,
stesso child1033119/epoca1, code0 e drop0. Il nuovo segmento
`20260928-220000.mp4.tmp` cresce da8.887.375 a25.664.591byte; è ancora aperto,
non è prova di MP4 chiuso/decodificato. Ispezione canonica autenticata delle
00:06:54 CEST PASS: Tracker9106frame, età frame28,65ms, recorder5363scritture
video archive, code0/drop0, temperatura massima79,685°C. Task controllati
6/11/14: un solo start, zero exit; recorder stessa generazione1. Misura esatta
9395entry/551756611byte PASS e quattro hash config identici alla baseline.
Recovery nativo PASS e risposta successiva non trasformano il soak FAILED in
PASS, né provano assenza di stall intermittenti del supervisore.

Nel campione compaiono checkpoint recorder lenti (ultimo/massimo sync6419ms)
e swap; nessun OOM nei riepiloghi runtime. Questi sono indizi, non una causa
provata del timeout. Non sono emersi eventi `supervisor_health_socket_slow`
o `supervisor_health_socket_failed` nel journal della finestra ispezionata.
La diagnosi del gap di osservabilità resta nel consolidamento corrente:
[issue health/qualifica](../../setup/issues/2026-09-29-test-board-supervisor-health-timeout-night-soak.md).
Non attribuire il fallimento ai13FPS notturni o alla termica senza evidenza.

## Valutazione della proposta

La direzione è corretta: **un rallentamento dell'analisi non deve interrompere
la registrazione**, e un broker vivo senza cattura deve essere osservabile e
recuperabile. Non basta però aumentare timeout, cancellare `frameCleanupErr` o
riavviare: il blocco protegge anche da un rilascio hardware non confermato.
Occorre separare isolamento dei flussi, completamento della pulizia e recupero
della piattaforma. La causa iniziale dello stallo resta da provare; il codice
conferma invece il meccanismo che lo amplifica in un fermo persistente.

## Sintomo

- `recordings/video/` vuota; `recorder` con `last_video_unix_milli` fermo alle
  11:38:54, `last_raw_video_write_unix_milli` fermo alle 11:30,
  `thumbnail deferred ... reason=video_ingress_stale` ogni minuto.
- L'**audio continua** (`recordings/audio/` in scrittura, `last_frame_unix_milli`
  corrente): il recorder non è completamente fermo, ma questo non esclude un
  problema del percorso video. Resta `state=recording` con
  `video_enabled=true`, `raw_dropped_total=0`: stato insufficiente a descrivere
  la registrazione effettiva di ciascun media.
- Nessun processo di cattura vivo: `pgrep -af rdkx5_v4l2_mux_bridge` vuoto.
- `Task 6 (Python - AI Tracker)` in crash-loop ogni ~2 s dalle 11:41:35
  (6.520+ restart al momento della diagnosi), con
  `frame startup stage=source_pending ... admission failed`.

## Catena dei fatti (con evidenza)

1. **11:38:31 → 11:38:54, stallo progressivo dell'invio sullo stream
   `analysis`.** Il bridge registra quattro `event=source_output_slow` con
   `wall_us` crescente: 360.872 → 1.265.847 → 1.803.039 → 3.002.849. L'ultimo:
   `stream=analysis origin_sequence=770804 wall_us=3002849 thread_cpu_us=524
   prepare_us=15 integrity_us=2250 records_us=3000580 authority_wait_us=1
   socket_us=3000523 records_sent=4 payload_bytes=345600 code=-5`.
   Il tempo misurato è quasi tutto nell'invio socket (3,000 s) con 0,5 ms di
   CPU del thread: compatibile con contropressione o mancato avanzamento del
   ricevitore. Non identifica quale fase a valle si sia bloccata e non misura
   la CPU complessiva né esclude ritardi di scheduling/I/O.
2. **Il budget di trasporto scade**: `code=-5` = `FRAMEPEER_DEADLINE`,
   `peer_stage=io_deadline`, `event=frame_source_failure
   stage=output_send code=-5`. Il budget coincide con quello corrente (3 s,
   `VB_ANALYSIS_SEND_BUDGET_MS`); ciò non prova che sia adeguato a ogni stallo.
   Aumentarlo da solo prolungherebbe il blocco nel percorso condiviso.
3. **Un solo stream stalla, ma si ritira l'intera sorgente**:
   `action=retire_source`, poi `action=close` e chiusura UVC
   (`event=uvc_capture_closed ... streamoff_rc=0 close_rc=0`). Poiche' live
   (`main`), analisi (`sub`) e `archive` vivono nello stesso processo,
   **la registrazione muore con l'analisi**:
   `event=capture_epoch_ended outcome=failure reason=frame_source_failure:output_send:io_deadline`,
   `duration_ms=38453845` (l'epoca 2 era viva da 10,7 h).
   **Orario da ricorrelare:** 11:38:54 è l'ultimo video riportato; nel codice
   l'evento di fine epoca viene emesso dopo attesa del figlio e pulizia. Non
   attribuirgli lo stesso timestamp senza riconciliare il KILL successivo.
4. **Il broker non riesce a confermare la pulizia della cattura.** Dopo il
   fault il figlio non esce da solo e il broker lo escalationa
   (`event=capture_child_escalated child_pid=4051208 signal=KILL grace_ms=10000`,
   11:39:11). La pulizia interna ha un budget di **1 s**
   (`broker.go:5179-5188`): `11:39:11 frame_source
   stage=native_surfaces_retired source=front source_epoch=2 staged_frames=770625
   drops=68` seguito nello stesso secondo da
   `frame_source stage=cleanup_unconfirmed media_epoch=2 child_pid=4051208
   err=context deadline exceeded`.
5. **Il fallimento di pulizia e` permanente per design.** `rt.frameCleanupErr`
   e` sticky: `ensureChildLocked` (`broker.go:1377`) rifiuta ogni nuova
   ammissione ("frame source cleanup unconfirmed", `action=hold_admission`) e
   **nessun percorso lo azzera** (unici due assegnamenti: `broker.go:1777` e
   `broker.go:5187`). Il recovery del broker ritenta ogni 3 s
   (`event=capture_recovery_scheduled ... retry_in_ms=3000`) e riceve sempre lo
   stesso rifiuto: 454 `capture_recovery_start_failed` in 4 h. Il restart
   eliminerebbe il latch in memoria, **non proverebbe il rilascio dell'SDK**.
   `NativeFrameExecution.Release` accetta come ricevuta di rilascio soltanto
   uscite normali 0/64; un SIGKILL non soddisfa questo contratto. Anche una
   chiusura UVC pulita non certifica il rilascio di tutti gli acceleratori.
6. **Conseguenze a cascata.** Nessuna nuova epoca di cattura → nessun video in
   archivio → il recorder non ha nulla da scrivere. Il tracker perde la
   sorgente e crash-loops (`stage=source_pending`, `PeerError: frame peer
   stage=receive_record code=-6`), 6.520 restart.
7. **L'auto-recupero esisteva ed era esaurito.** Il supervisore escalation a
   restart completo dopo 40 restart senza stabilizzazione
   (`main.go:7246-7258`), con budget **3 per boot** in
   `/run/nemesi/supervisor_escalations` (runtime dir, normalmente tmpfs). Il ledger vale **3**:
   consumato tre volte dallo **stesso** task 6 il 22/09 06:58, 22/09 13:00 e
   23/09 17:56 (ogni volta il servizio e` stato riavviato: e` da li` che viene
   l'`ActiveEnterTimestamp` del 23/09). Oggi la macchina non e` stata riavviata
   (boot 2026-08-26) e ogni soglia di 40 restart ha prodotto
   `escalation budget exhausted for this boot`: nessun restart completo, quindi
   la presa sticky non e` stata rilasciata.
8. **Telemetria incompleta e recupero inefficace**: l'ultimo
   `video_pipeline_summary` e` alle 11:38:37 (il broker smette di pubblicarlo
   senza produzione di frame). La salute espone invece `degraded`: non è
   assenza totale di diagnostica. Il commento "must not trigger source
   recovery" riguarda frame freschi ma FPS inferiori all'obiettivo, **non**
   il fermo della cattura. Il watchdog del figlio non risolve l'ammissione
   bloccata senza figlio; i retry falliscono e l'escalation generale esaurita
   non fornisce un recupero utile.

## Evidenze che limitano le ipotesi, senza escluderle tutte

- **Spazio disco**: `/var/lib/nemesi` non e` un mount separato; la radice ha
  16 GB liberi su 58 GB (73%). Il tetto `max_bytes=16106127360` e` una quota
  del recorder, e la retention **lavora** (run 228/229/233 con
  `deleted_groups=1 deleted_files=7`). Questi dati non indicano ENOSPC, ma non
  escludono latenza/contesa I/O. `before_bytes==after_bytes==16002355222` con
  `protected_groups=2` descrive i gruppi protetti, non prova l'assenza di stalli.
- **Camera/UVC**: l'epoca 2 ha lavorato 10,7 h a 20 fps con
  `capture_timeouts=0 capture_corrupt_frames=0`; la chiusura UVC e` pulita
  (`streamoff_rc=0`). Non emerge un errore UVC come innesco; restano da spiegare
  il mancato termine del processo e il rilascio SDK non confermato.
- **Recorder**: continua a registrare audio; nessun errore di finalizzazione
  (`native archive finalized base=20260924-093000` alle 11:40:01). La chiusura
  di un segmento precedente non dimostra che arrivino nuovi frame video.
- **`main_frames=0` in tutti i 700 summary del giorno non e` un guasto**: il
  contatore avanza solo sotto `publish_main` /
  `direct_main_native && publish_main` (`rdkx5_v4l2_mux_bridge.c:4298,4857`),
  cioe` quando l'uscita live e` richiesta da un client. Nessun client live era
  collegato; il ramo live non e` stato esercitato oggi.

## Non determinato

Chi non drenava il socket del broker in quei 3 s. Il lato tracker riportava
(`frame_reader stage=connection_retired grant=tracker delivery=true
received=1061125 completed=1061124 idle=2`): contatori cumulativi che non provano
avanzamento nella finestra critica. Correlare ricezione, integrità,
pubblicazione, delivery, scheduling e I/O prima di attribuire la causa a
Tracker, broker o storage. Poco prima il recorder era in pressione
di scrittura (`native_archive_storage_write_slow write_ms=4512` alle 11:35:50,
`archive_checkpoint_slow_total=343`): e` un concorrente plausibile, non provato.

## Lavoro recente da mantenere e completare

- `398dfa96`: integrità e diagnostica IPC più esplicite, inclusi i progressi
  di ricezione per fase (`sourceReceiveProgress`), e limite di escalation per
  boot. Riutilizzare queste evidenze; la protezione anti-loop resta necessaria.
- `78b42cd2`: budget frame più ampi e tolleranza limitata alle discontinuità
  H.264/H.265 con attesa di un nuovo IDR. Quest'ultima non risolve l'ingresso
  MJPG di deployed board e non isola l'uscita analysis.
- `fec51c45`: rimozione di ulteriori budget da un secondo, incluso l'invio
  analysis a 3 s. Utile per stalli brevi, non elimina il percorso bloccante.
- `native/source_publisher.c` ha già pubblicazione asincrona bounded delle
  superfici native, con slot sostituibili. Non confonderla con l'uscita NV12
  analysis, ancora sincrona nel ciclo di cattura di
  `native/rdkx5_v4l2_mux_bridge.c`.

Non serve riscrivere FAP né rinunciare a hash, ownership o correlazione dei
crop: manca soprattutto l'isolamento della consegna e il recovery a stati.

## Piano correttivo proposto

### 1. Isolare i flussi senza indebolire i contratti

Responsabilità del video-broker standalone, con semantica comune a tutte le
piattaforme e implementazioni verificate per ciascun provider hardware.

- Rimuovere l'attesa del consumatore analysis dal ciclo camera/decoder/encoder
  condiviso: consegna per stream con buffer immutabili preallocati, limite
  esplicito di slot/byte e ownership fino al termine dell'uso. Non aprire tre
  lettori USB o duplicare decoder per simulare indipendenza.
- Per analysis/live privilegiare la freschezza: sostituire/scartare frame
  completi non ancora impegnati nella consegna, contabilizzando le perdite.
  Misurare copie, CPU/frame, memoria, FPS e latenza; una coda lunga non è una
  soluzione. Anche lock e risorse condivise devono avere attese limitate.
- L'invio fallito può essere **parziale** (`records_sent=4`). Non ignorare
  l'errore e proseguire sullo stesso canale: ritirare la sessione compromessa,
  revocare i grant interessati e ricreare il canale dello stream. L'attuale
  `frame_source` fallisce a livello sorgente: separarne i domini di errore,
  non aggiungere soltanto un thread attorno alla stessa sessione condivisa.
- Preservare origine, sequenza, epoch della sorgente e generazione dello
  stream. Un rebind analysis non inventa un nuovo frame/capture epoch; uno
  switch/restart reale invalida i vecchi riferimenti. Un ROI scaduto non può
  essere servito usando il frame più recente come se fosse quello richiesto.
- Archive conserva una politica propria: nessuno scarto arbitrario di slice
  interdipendenti. Se l'archiviazione non tiene il passo, dichiarare il gap,
  recuperare su un punto decodificabile e preservare segmenti e sincronismo
  audio/video. Non promettere registrazione senza perdite con storage fermo.

Il guasto di un consumer deve degradare quel consumer. Un guasto reale della
camera, del decoder o di una risorsa hardware comune può invece coinvolgere
più flussi: va classificato, non nascosto né convertito in fallback software.

### 2. Recovery tipizzato: attesa scaduta non significa rilascio fallito

Separare almeno `running`, `stream_degraded`, `recovering`, `cleanup_pending`,
`quarantined` e `blocked_requires_intervention`, con causa e scadenza esplicite.

- `framedomain/resource.go: WaitClosed` distingue la scadenza del chiamante dal
  risultato dell'operazione. `Retire` oggi memorizza anche un timeout della
  prima attesa: consentire di osservare il completamento della **stessa**
  pulizia, senza riavviarla, duplicare release o perdere le lease pendenti.
- Applicare un termine complessivo anche al join `childFramesDone`, oggi
  illimitato prima del budget di un secondo. Se il join non termina, segnalare
  quarantena; non liberare memoria/hardware ancora in uso. Aumentare soltanto
  quel secondo non risolve un join bloccato o una ricevuta SDK mancante.
- Dopo pulizia confermata e join completo, consentire una nuova generazione.
  Se invece il processo termina con KILL o il provider non conferma il
  rilascio, mantenere la quarantena: il semplice restart del broker non può
  trasformarla in autorizzazione a riutilizzare hardware potenzialmente attivo.
- Conservare questa condizione fuori dalla memoria del broker, in uno stato
  operativo autorevole che sopravviva ai restart pertinenti, non in
  `/opt/nemesi`. Sbloccarla solo con prova di rilascio o reset verificato del
  provider, coordinato con gli altri utilizzatori dello stesso acceleratore.
  In assenza di reset supportato: allarme e intervento, non retry infinito.
- Registrare prima dell'escalation fase del teardown, risorsa/lease pendente,
  motivo del KILL e risultato del provider. La camera UVC chiusa non basta a
  certificare JPU/VPU/BPU o altri motori; neppure un reboot va assunto come
  reset di ogni periferica senza verifica della piattaforma.

### 3. Sorvegliare il servizio reso, non soltanto il PID

- Estendere il watchdog esistente alla cattura richiesta ma senza figlio o
  senza ammissione. Non introdurre controller concorrenti che si riavviano a
  vicenda. Il broker governa le risorse; il supervisore integra stati e azioni
  generiche, senza inserire Task 6/Task 14 o logiche Registry nel broker.
- Heartbeat indipendente dall'arrivo dei frame, con stato per stream, ultimo
  progresso monotono, domanda, motivo di stop, recovery in corso e deadline.
  Un osservatore esterno deve rilevare anche heartbeat bloccato: il controllo
  non può dipendere dallo stesso pump/lock che sta sorvegliando.
- Distinguere video richiesto ma fermo da live senza spettatori, configurazione
  disabilitata, stop termico e aggiornamento. FPS inferiori ma frame freschi
  non autorizzano automaticamente il riavvio della camera.
- Recorder: stato per media e contatori separati di ingresso, scrittura e
  finalizzazione. Audio attivo + video fermo deve risultare degradato in API/UI
  e allarmi, non semplicemente `recording`. Il solo ultimo frame ricevuto non
  prova una registrazione persistita e decodificabile.
- Tracker in attesa della sorgente: stato `waiting_source`, backoff e ripresa
  all'ammissione; non migliaia di nuovi processi né consumo di budget per il
  guasto già noto di una dipendenza. Non mascherare errori propri del Tracker.
- Budget per dominio di guasto e finestra temporale, mantenendo un tetto
  globale anti-loop e reintegro soltanto dopo stabilità **funzionale** misurata.
  Per-task da solo non basta: le tre escalation erano dello stesso task.
  Persistenza atomica e comportamento conservativo se il ledger non è
  scrivibile; il contatore solo in memoria si perde col restart del processo.
- Correlare `source_output_slow`, `source_receive_progress`, pubblicazione,
  lease e storage con incident ID, source epoch, stream generation e sequenza.
  Metriche aggregate e log limitati: non creare ulteriore pressione sulla SD.

### 4. Storage e ripristino operativo: separati dalla correzione primaria

Il 99,4% riguarda la quota del recorder, non l'occupazione fisica della SD.
Valutare headroom/retention su gruppi chiusi in base a durata e bitrate, senza
cancellare gruppi attivi o dati utili; misurare separatamente latenza I/O,
spazio fisico e pressione sulla quota. Non presentare la retention come causa
provata né come rimedio sufficiente all'accoppiamento analysis/archive.

La precedente proposta di kill del PID o restart del servizio **non è una
procedura di recovery sicura**. `systemctl restart nemesi-agent` non azzera il
ledger per boot. Il ripristino autorizzato richiede stato corrente,
identità dei processi, condizioni hardware e percorso canonico Safe Deploy;
non usare PID storici né cancellare latch/ledger per eludere una quarantena.
Non eseguire esperimenti o stress test su deployed board per qualificare la soluzione.

## Sequenza operativa e prove di accettazione

1. **Riprodurre su test board/fixture**, partendo dal caso analysis bloccata oltre il
   budget; raccogliere le fasi già strumentate. Separare l'innesco dello stallo
   dal difetto, già individuato, che spegne la sorgente e impedisce il recupero.
2. **Un intervento coerente**: isolamento per stream, teardown riconciliabile e
   osservabilità/recovery insieme. Iterazione mirata su test board, preservando e
   riconciliando gli hotfix nel repository; niente deploy documentali.
3. **Qualifica integrata e soak**, mantenendo le prove già valide. Coprire i
   profili centrali e i provider effettivi: H.265 in ingresso su test board non prova
   il percorso MJPG/JPU/VPU/BPU di deployed board. Proposta aggiuntiva: almeno 24 h su
   test board includendo rotazione e retention, poiché il caso è emerso dopo 10,7 h.
   Questa proposta non modifica automaticamente goal o soglie vigenti.
4. **Un rilascio consolidato su deployed board**, solo dopo qualifica e verifica del
   percorso specifico, con backup/rollback governati e poche scritture.
   Verificare il servizio reale e il soak di deployed board senza fault injection sul
   dispositivo cliente. Il recovery operativo iniziale di deployed board è già avvenuto;
   il rilascio della correzione resta subordinato alla qualifica.

Matrice minima di regressione:

| Scenario in ambiente di test | Risultato necessario |
| --- | --- |
| Consumer analysis lento, arrestato o bloccato; richieste ROI concorrenti | Archive e live richiesto avanzano; code e memoria limitate; analysis recuperabile senza restart della camera. |
| Invio parziale, disconnessione, frame corrotto o replay | Canale interessato ritirato; nessun frame misto, origine falsificata o contaminazione dei crop. |
| Pulizia termina dopo la prima scadenza di attesa | Completamento riconciliato una volta, senza doppia release; nuova generazione solo dopo conferma. |
| SDK bloccato/KILL, poi restart del broker/supervisore | Nessun riuso senza prova hardware; reset verificato oppure stato bloccato con allarme. |
| Incidenti ripetuti in boot lungo, budget esaurito o ledger non scrivibile | Recuperi consentiti secondo politica, limite globale effettivo e nessuna tempesta di restart Tracker. |
| Storage lento, pieno o read-only su fixture/test board | Distinzione da cattura ferma; gap espliciti e segmenti decodificabili, senza falsi stati sani. |
| Idle, stop termico, aggiornamento, switch e restart | Nessun falso watchdog; rispetto delle soglie e invalidazione corretta dei vecchi riferimenti. |

Prima delle prove fissare `T_detect`, `T_recover` e `T_alert` per classe di
guasto/profilo, coerenti con i watchdog esistenti: non un generico “N minuti”,
né soglie allargate dopo un fallimento. Il recovery recuperabile deve stare
entro `T_recover`; quello non recuperabile deve essere dichiarato e notificato
entro `T_alert`, senza tentativi infiniti.

**Chiusura dell'issue:** matrice superata con evidenze referenziate; avanzamento
video osservato fino a segmenti nuovi persistiti e decodificabili con A/V
coerente; nessuna interruzione archive dovuta al solo consumer analysis;
assenza di regressioni FPS/CPU/RAM/latenza/termiche rispetto al profilo e alla
baseline; qualificazione test board e conferma su deployed board. PID vivo, audio corrente e
directory non vuota non bastano. Nessun allentamento delle soglie termiche,
fallback software implicito o cancellazione del corpus per superare i test.

Non è possibile garantire “mai più” per guasti fisici, kernel, alimentazione o
storage. La garanzia verificabile è più precisa: **un guasto isolato di un
consumer non abbatte gli altri flussi; un guasto comune non può lasciare il
sistema indefinitamente fermo e silenzioso o dichiarato sano**.
