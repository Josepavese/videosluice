# test board: temperatura residua elevata con encoder live fermo

Stato: **APERTO, causa residua non isolata.** Il consolidamento a94 test board/deployed board
e' stato chiuso il7settembre; non riaprire quei risultati storici. Il nuovo
candidato FAP b0211cc8 fallisce invece il limite termico il10settembre: residuo
attivo N6, oltre all'indagine causale B1. Dettagli piu' recenti in fondo.
Ambito: qualificazione fisica RDK X5 test board e, dal13settembre, deployed board con
evidenza propria distinta; non una diagnosi applicabile per supposizione a
tutte le piattaforme o camere. Nessuna nuova modifica runtime.

## 13settembre: test board consolidato PASS, deployed board fallisce il limite termico

Aggiornamento16:24UTC: l'utente conferma che deployed board e' a posto dal punto di
vista hardware. Registrare questo come riscontro dell'operatore, non come
telemetria remota di ventola/dissipazione (assente). Non attendere un ulteriore
controllo fisico per avanzare: priorita' alla diagnosi del carico software,
trasferimenti/sincronizzazioni degli acceleratori e costo della raccolta soak.
La conferma non annulla il fallimento misurato, ne' identifica da sola un leak.
Nessuna nuova modifica di profilo, soglie, OS o runtime.

Aggiornamento16:02UTC: una prima ottimizzazione e' nel checkout, NON installata.
V4L2/GStreamer non calcolano piu' la misura post-tone su frame per cui nessun
log la usa. Guardia RAW e observer qualita' indipendente invariati. Test sul
worker reale RED->GREEN, pixel equivalenti, transizione al nero non ritardata;
suite native/ASan/UBSan e GStreamer CPU PASS. Benchmark isolato test board risparmia
0,86-0,91ms/frame di CPU per720p/stride8: non dimostra che il calore sia risolto.
Campione deployed board30s: CPU host29-31%, GPU On~12,75%/Suspend~87,25%, principale
RSS stabile nella finestra. Non prova assenza generale leak o raffreddamento.

test board ha superato2700,116s/5379campioni, Tracker19,606FPS e max87,257C sul
consolidato0e98f5b5. Non riaprire quella prova per il risultato diverso deployed board.
deployed board stesso codice/binari, profilo MJPG720p20/tone OpenCL, ha fallito dopo
1813,084s/3590campioni: thermal_zone0 90,139C, limite qualifica90C invariato.
Il report segnala inoltre due overrun di raccolta nelle fasi systemd.

La camera mantiene20FPS e archive15; senza spettatori live_active=false,
live_fps0, encoder_epoch0. A90,1C il governor riduce analysis13 e rilascia
OpenCL; a86,9C ripristina20/OpenCL, poi la temperatura torna al limite.
Questa correlazione non isola GPU, Tracker, CPU, ambiente o dissipazione.
GPU/BPU performance996MHz e DDRperformance4,266GHz sono coerenti con
configurazioni gia' osservate, non una nuova modifica dimostrata.

Consolidato deployed board d4e02a2e ancora attivo: stesso supervisor976595/invocation,
NRestarts0, misura esatta9319entry/553265236byte PASS dopo il fallimento,
quattro impronte configurazione immutate. Nessun rollback automatico del
controller soak, nessun nuovo deploy o tuning eseguito. Non modificare
soglie o gate FPS per chiudere N6; prima isolare il residuo termico e poi
qualificare una sola soluzione pertinente. Storico e ricevute nella
[milestone](../../../improvements/INTERMEDIATE_RELEASE_2026-09-12.md).

## Confine verificato

Il successore firmato `a571c3cd350eed3fb0cd3a8971c177fc4ef86c01c02cb4c39bd5729163d0c2bf`
ha superato la matrice di switch e la verifica del controllo live post-cutover.
Il suo soak del 6 settembre, 13:04:46-13:06:46 Europe/Rome, e' pero' fallito
sul gate termico canonico >=90 C. Non confondere questo esito con il precedente
soak notturno verde di un altro artefatto, ne' con una regressione tracker.

La ripresa del 6 settembre, 13:39-13:46, effettua soltanto letture tramite
wrapper SSH canonici e Python isolato/no-bytecode. Nessun deploy, riavvio,
stress, cambio soglia/governor/profilo, modifica di boot o stop del servizio.
I client di test erano gia' stati fermati nel turno precedente.

Identita' invariata: agent PID2768276, invocation
`8baa361bbbf240b4b1c582c2189bb7c7`, NRestarts0, capture overview PID2775325.
Piano temporaneo85 OOO; conservare l'intento precedente FOO e ripristinarlo
soltanto con campione valido, preview e CAS, senza sovrascrivere eventuali
nuove decisioni operatore. A 13:46:27 lo snapshot autenticato ha IPC validation0,
authentication0; tracker running e recorder recording, zero drop/finalize error.

## Misura fisica 13:43:16-13:43:46

- CPU media complessiva15,559%, iowait0,424%, campione30,128s. Le percentuali
  per processo sono su singolo core: tracker36,98%, capture22,80%, audio14,60%,
  Person Registry12,94%, broker8,96%, recorder3,42%. La sonda vale0,56% di un core.
- DDR91,458-92,289 C, CPU90,677-91,703 C, BPU90,433-91,116 C.
- BPU24-32%, VPU6,4-6,8%, GPU3D/GPU2D/JPU0% nei campioni disponibili.
- VPU: live H2651280x720 input/output18695->18695; archive H265640x360
  47434->47895; decoder H2651920x1080 94224->95128. Live fermo, decoder e
  archivio in progresso; nessuna coda encoder pendente nelle due letture.
  Il campo FPS della tabella live conserva21: non usarlo come misura corrente
  quando i contatori non avanzano. La sorgente overview attuale e' H265 via
  USB/VPU, non MJPG/JPU; il profilo MJPG richiesto per deployed board resta distinto.
- Dominio GPU off e JPU sospesa alle letture iniziale/finale; JPU runtime
  usage0, GPU3D/2D usage0. GPU3D active_time cresce di100ms nel campione:
  non dichiarare che non sia mai stata attivata fra due osservazioni.
- Memoria GPU invariata75784byte di pool system e0byte virtuali. Tracker e
  capture hanno RSS/thread stabili nei30s. Due inventari DMA-BUF riportano
  entrambi77oggetti/137826304byte; gli heap ION non riportano orphaned buffer.
  Queste finestre brevi non certificano l'assenza di ogni leak o frammentazione.
- MemAvailable circa1,94GiB, swap libero invariato. CmaFree circa16MiB va
  interpretato insieme alle riserve/heap, non come prova isolata di perdita.

Alle13:46 test board e' ancora91,947 C DDR/91,214 C CPU. Live inactive/zero clienti,
analysis13,25fps sul cap13, archive15,25fps. Il cap termico applicativo agisce;
il riscaldamento residuo non e' spiegato dal solo vecchio difetto live.

## Power management: confronto, non tuning

Le osservazioni correnti concordano con l'audit locale del5settembre:
CPU schedutil300-1500MHz, boost0, BPU/GPU performance nominale1GHz,
DDR performance4266MHz; driver CPU idle assente gia' nel kernel precedente.
Le protezioni kernel sono step_wise, throttling95 C e critica CPU110 C.
Sono soglie diverse dal gate di qualificazione90 C: nessuna e' stata cambiata.
Non e' emersa una nuova configurazione energetica introdotta dal deploy.
Questo non esclude modifiche transitorie non registrate o problemi fisici.

La documentazione ufficiale descrive DVFS per CPU/GPU/BPU/DDR e avverte del
compromesso fra consumo e prestazioni. Non basta leggere una frequenza alta
per dedurre che un acceleratore sospeso stia elaborando. Nessun undervolt,
nuovo governor o modifica kernel e' giustificato da queste sole misure.

- [RDK X5 low power](https://developer.d-robotics.cc/rdk_x_doc/en/Advanced_development/linux_development/driver_development_x5/driver_lowpower),
  Needle-X `trace_03fa25776e0df3d2`.
- [RDK X5 thermal](https://developer.d-robotics.cc/rdk_x_doc/en/Advanced_development/linux_development/driver_development_x5/driver_thermal_dev),
  Needle-X `trace_2b266c85f01c0bbd`; i valori effettivi sono letti dal device,
  non copiati dagli esempi della pagina.

## Prossimo passo e limiti

Serve riscontro su ventola, contatto dissipatore, contenitore, ventilazione
e temperatura ambiente. Il device non espone telemetria fan utile: da remoto
non e' stato verificato che la ventola giri, ne' dimostrato che sia guasta.
Non sostituire questa verifica con ripetizioni del soak su un device gia' caldo.
Un eventuale confronto causale con arresto temporaneo dei servizi o interventi
sul raffreddamento va pianificato separatamente; non e' stato eseguito qui.

deployed board resta su `fe23695d...`, stesso agent/NRestarts0: alle13:39 DDR87,208 C,
CPU86,378 C, analysis20fps, archive15fps, live inattivo. Nessuna modifica.
Solo dopo il chiarimento termico e test board qualificato, promuovere lo stesso ZIP
firmato e completare il postflight/soak deployed board. Goal generale non completato.

## Nuova verifica e qualificazione pomeridiana, 14:41-15:41

Alla richiesta dell'utente di verificare e aggiornare se possibile, test board e'
sceso a82,567C DDR/81,541C CPU. Misura esatta verde:8929entry/547904530byte;
artefatto `a571c3cd...`, preset centrale v7, stesso agent/invocation/NRestarts0,
IPC strict valido e piano85OOO. deployed board e' invece89,114C DDR/88,381C CPU.
Il nuovo soak r2 parte soltanto dopo questo preflight e due reader ammessi:
nessun cambio di soglia/profilo, arresto dell'agent o azzeramento dei contatori.

Il report canonico v3, root0600, riletto e autenticato indipendentemente,
`/var/lib/nemesi/setup/recovery-soak-results/50328334601aef24f56d09cb4819722c.json`
riporta **passed** sullo stesso `a571c3cd...`, sequenza1788688828156870092,
transazione `1584a828335421370b4488a59a7f903c180646ceaea8f2837bcfb9ff53ef6fe6`:

- 14:45:32-15:30:33 Europe/Rome,2700,109s,5401campioni; massimo84,961C.
- Zero collection overrun; gap massimo fra completamenti0,631s.
- Tracker+48943frame, archive+40507video/+134880audio, cinque finalizzazioni;
  zero errori di finalizzazione, drop archivio o errori retention aggiunti.
- Diciassette checkpoint lenti, sync massimo6639ms: diagnostica persistente,
  non certificazione dello stato fisico della SD.
- I due reader SDK completano3000,139s e3000,125s,75078/75086frame,
  circa25,026fps effettivi; gap massimo0,109s contro limite2s, bootstrap
  VPS/SPS/PPS+IDR, inode invariato. Eta' frame a regime p95 circa2ms e massimo
  54,56ms: misura locale SDK, non latenza ottica/rete/browser.

Le unit `nemesi-operator-live-a571-test-board-r2-{a,b}` e
`nemesi-operator-video-soak-a571-test-board-r2` terminano tutte con successo.
Il loro stop incrociato in caso di errore riguarda solo questi tre test;
nessun processo Nemesi permanente viene fermato. Stesso carico OOO del
tentativo fallito, live configurato30fps: i25fps misurati non sono una riduzione
introdotta per superare il test, ne' prova che la camera eroghi30fps.

Finito il carico di prova, ripristinata la frontale alle15:36:53 con preview,
CAS85 e singola operazione `a571-r2-restore-front-apply`, conclusa `succeeded`.
Piano86FOO, analysis/archive generazioni18/39 invariate, live48. Confronto
autenticato prima/dopo tutto verde: stesso agent, boot, task, config e release;
overview PID/epoch, endpoint/epoch/generazioni analysis/archive preservati;
nessun incremento drop, disconnessioni o errori recorder.

deployed board rimane sulla baseline `fe23695d...`, agent3082516, invocation
`ee1cc6a9a14041c7a3d1ff44b00703db`, NRestarts0; il preset centrale v4 riletto
conferma MJPG1280x720@20/JPU, live H2651280x720@20/VPU, analysis NV12
640x360@20 e archive H265640x360@15 AVBR750/VPU, con BPU per il tracker
e senza fallback software.
Alle15:36:44 DDR90,530C/CPU89,846C: **nessun nuovo upload o deploy deployed board**.
La promozione dello stesso ZIP ora ha una qualificazione test board verde, ma il
destinatario necessita di nuovo preflight termico valido; il sandbox attende
se parte da86C o piu', richiede83C dopo cooldown e interrompe a90C.
Nessuna soglia cambiata e nessuna pubblicazione al canale automatico.

Il successo pomeridiano non cancella il fallimento di mezzogiorno e non
dimostra da solo la causa o la risoluzione del riscaldamento intermittente.
I limiti di diagnosi fisica, SD,30fps, PTZ e copertura browser restano distinti
dal risultato positivo del soak; nessun nuovo test browser pesante eseguito.
Evidenze: `/tmp/nemesi-update-recheck.xZGZomvZ/`, in particolare
`test-board-soak-verified.jsonl`, `test-board-readers-results.log`, `test-board-observation.jsonl`,
snapshot prima/dopo ripristino, `test-board-restore-progress.jsonl`,
`deployed-board-central-preset.json`, `deployed-board-identity.json`, `deployed-board-final-temperature.log`.

## deployed board: retry autorizzato e deploy riuscito, 16:01-16:50

Il nuovo controllo richiesto dall'utente trova deployed board88,088C DDR/87,599C CPU;
alle16:04 DDR87,941C. La verifica del percorso firmato chiarisce la distinzione
operativa:86C e' il limite di avvio del build sandbox, non un divieto globale
di iniziare l'updater. Prima delle fasi pesanti l'updater ferma ordinatamente
il runtime; il sandbox aspetta83C se inizialmente caldo, con timeout15min e
abort90C. Nessuna di queste protezioni viene modificata o aggirata.

Preflight: release baseline integra8913entry/547771559byte, stesso agent,
IPC3/auth0 invariato e attribuito al difetto raw-query gia' corretto nel
candidato; preset centrale v4 riletto e identico al device. Il report test board
`50328334601aef24f56d09cb4819722c` e' autenticato nuovamente sul suo agent
e sul medesimo ZIP `a571c3cd...`, sequenza1788688828156870092.
Un solo deploy ufficiale, senza rebuild del pacchetto:

- Staging `20260906160511-528901-65WhDQrf` e corrispondente unit
  `nemesi-update-20260906160511-528901-65WhDQrf.service`.
- Transazione `f4a9f598f50ea44829070e9676af9842eeb2680d54fe1c59ce5ab77947c53118`.
- Runtime fermato ordinatamente; alle16:11:43 circa85,2C.
- Python sandbox avviato84,815C,301s/rc0; compile avviato82,518C,559s/rc0.
  Solo lo staging della cache opzionale compile e' saltato per budget aggregato;
  nessun modulo o test obbligatorio saltato. La promozione di441100631byte
  di risultati prosegue su disco e termina; nessun processo ucciso manualmente.
- Test AI/eventi, percorso RDK X5 BPU e modelli multilingua positivi.
  Avviso SDK/modello YOLO-world hbrt3.15.55 contro tool3.15.49 conservato nel log:
  non convertire l'avviso in una certificazione di equivalenza numerica.
- Una lettura diagnostica systemctl supera10s; una successiva apertura SSH
  scade nel banner. L'osservatore e il controller tornano a ricevere risposte
  sulla stessa invocazione. PSI non esposto dal kernel: nessuna conclusione
  sulla salute della SD basata su dati di pressione assenti.
- Controller termina **rc0 alle16:45:35** con release e postflight verificati;
  boundary recovery esterno finalizzato. Transazione conclusa: NON usare resume.

Postflight indipendente, ore16:46-16:50:

- `a571c3cd...` healthy/applied, misura esatta8929entry/547904514byte verde.
- Token con fingerprint invariati e preset v4 con hash canonico invariato;
  nessun aggiornamento OS o cambio boot, rete o profilo centrale, nessuna pubblicazione
  al canale automatico globale.
- Agent1173382, invocation `62e4262d74ab45c5a7148aa66822913e`, NRestarts0,
  piano1FFF, IPC0/auth0 e activation inhibitor assente.
- MJPG/JPU20 attestato, decode fallback none; archive H265/VPU attestato.
  Live senza spettatori e zero frame: non e' un nuovo test browser o della
  produzione live20fps. Le risoluzioni/rate desiderate restano quelle del v4.
- Recorder inizialmente idle nel primo campione; nel campione strict finale
  e' writing con506video/1776audio, zero drop/errori finalizzazione. Tracker
  6772frame, reader/remap senza errori. Non confondere stream presente con
  persistenza: qui la scrittura e' verificata separatamente, ma nessuna
  finalizzazione lunga e' ancora stata qualificata nella nuova finestra.

Il successivo tentativo di avviare2700s di soak e' fermato nel preflight dal
gate termico su thermal_zone0 >=90C, PRIMA di creare una unit o un report.
`nemesi-operator-video-soak-a571-deployed-board-r1.service` e' not-found; non e' un soak
avviato/fallito o completato. Nessun carico video aggiunto, nessuna soglia
allentata. Alle16:50:38 deployed board torna87,599C DDR/87,648C CPU, campione strict
valido, ma analysis e' ancora sotto cap termico13 (ultimo osservato circa12fps)
contro configurazione20. Il profilo centrale20/20/20/15 non e' stato ridotto.

Il deploy e' riuscito e la nuova versione e' mantenuta; il riscaldamento
intermittente, i20fps sostenuti e il soak prolungato deployed board restano da verificare.
Non occorre ripetere il deploy per effettuare queste prove, ne' e' stato
disabilitato un rollback legittimo. Goal generale non dichiarato completo.
Evidenze: `/tmp/nemesi-deployed-board-deploy-retry.1oU5Tcfs/`, inclusi `deploy.log`,
identita'/misure prima-dopo, `deployed-board-final-sample.jsonl`, `deployed-board-final-health.log`,
`deployed-board-soak-start.log`, osservazioni del deploy e progressi delle fasi sandbox.

Evidenze fuori repository: `/tmp/nemesi-thermal-followup.Zj5vMhHQ/`
(`test-board-power-series.json`, inventari memoria, snapshot finale autenticato/IPC),
`/tmp/nemesi-package.kpD0oaaL/` (snapshot iniziali test board/deployed board e deploy/soak),
`/tmp/nemesi-test-board-thermal-audit.636mjtsG/POWER_MANAGEMENT_REPORT.md`
(confronto storico del5settembre, non prova corrente).

## 10settembre: b0211cc8, overrun nella nuova qualifica FAP

Promozione canonica development completata, riavvio01:38:26UTC, supervisor
770075/NRestarts0;72pin e misura esatta9283entry/552904179byte verificati.
Baseline9ffc/precedente dc59/dati/config preservati. Postflight15s/accesso PASS.
Routing97 invariato: overview H2651920x1080@30,analysis NV12640x360@20,
archive H265640x360@15 AVBR750; live inattivo senza domanda nel campione.
Il profilo deployed board MJPG rimane distinto e non e' stato toccato.

Prima osservazione autenticata60.051934s/60campioni PASS,max88.967C,1015frame
Tracker aggiunti,nessun errore lettura/validazione/uscita,stessoepoch1.
CPU %di un core:broker54.85,Tracker47.01,Registry27.01. Non una misura separata
delle inferenze BPU, della temperatura ambiente o un A/B causale.

Seconda osservazione prevista600s interrotta dopo79campioni: ultimo01:44:39UTC
a78.154s,frameTracker5999,errori0,nessuna uscita,Registryrunning/queue0/stalledfalse.
Massimo salvato89.846C; il verificatore rifiuta il campione successivo senza
riportare il numero. Il log indipendente del governor01:44:40UTC conferma
**90.2C/thermal_soft**,cap analysis13 e optional_compute non ammesso. Alle
01:45:40UTC torna nominale a86.8C,cap20; alle01:46:12DDR88.820/CPU89.309C.
Non era uno stallo video nei campioni raccolti, ma un vero fallimento del
limite termico. Non dimostra assenza di altri stalli o leak fuori dalla finestra.

Nessuna soglia modificata:qualifica<90C,governor90/95C. Non reiterare soak a
caldo; misurare risorse/power e costo causale FAP prima di nuova qualifica.
Il trasferimento storico in B1 non rende non bloccante questo nuovo esito N6.

Correzione LOCALMENTE testata nel verificatore di setup: errori distinguono
`kind=qualification_limit` con celsius/limite da `kind=invalid_sensor_value`.
Regressione RED->GREEN, intero contratto soak PASS incluse32reiezioni FAP;
90C/90.2C in gradi/milligradi e valori invalidi coperti. Non distribuita e
nessun deploy dedicato a questa diagnostica. Evidenze nel pack persistente
indicato da `improvements/FAP_WORK_HANDOFF.md`: `phase-qualified-observation*`,
`phase-qualified-thermal-stop-journal.log`, `thermal-rejection-diagnostics-*`.

Campione risorse successivo30.028944s sullo stesso b021: CPU %di UN core
broker55.28,EventCaptioning54.95,Registry46.95,Tracker44.99,nativecapture33.57.
CPU schedutil300-1500MHz,lettura1500MHz ai due estremi; nessun delta swap,
temperatureDDR/CPU89.114/89.651->87.941/87.501C. Nessun cambio governor
osservato rispetto al dato storico, ma non prova di stabilita' fra campioni.
Costi da isolare con prove comparabili; non attribuire il picco a GPU/BPU,
SD o leak sulla sola base di questa finestra. Risultato persistente
`phase-qualified-thermal-resource-sample.log`.

Audit10settembre02:09UTC: undici query principali dello stato Event Captioning
circa34.37ms CPU contro intervallo30s, zero scritture/letture fisiche del probe.
Non spiegano da sole il54.95% osservato; nessuna ottimizzazione speculativa
dello schema Event. Trovato invece un difetto nelle nuove ammissioni AI dopo
revoca del governor durante un batch o l'attesa del provider: quattro RED,
correttiva comune ai provider e199test PASS. Solo locale, nessuna nuova
qualifica termica o deploy; dettagli nella
[issue Event Captioning](../../event-captioning/issues/2026-09-10-background-ai-admission-revoked-during-batch.md).
