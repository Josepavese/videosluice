# Controllo live non riarmato dopo lo switch su un capture conservato

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. Controlli e domanda temporanea vengono riarmati/rilasciati correttamente dopo switch e rollback.

Evidenza: 1ecd: switch a zero viewer, park/resume e soak; a571: VPU live +0 in 101 s senza viewer. La causa termica residua resta APERTA.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


Stato: corretto nel successore `1ecd1414...` su test board; switch a zero viewer,
park/resume ripetuti e soak2700s con due consumer2850s verificati il2026-09-06.
Il successivo difetto di riuso dell'epoch e il suo correttivo sono descritti
nella [issue delle generazioni live](2026-09-05-live-epoch-repeats-after-encoder-replacement.md).
Il precedente `1e296ff0...` resta non promuovibile. deployed board non e' stato ancora
aggiornato con questo successore; lo storico sotto non descrive il suo stato.

## Evidenza e causa

test board, 5 settembre 2026, release `fe23695d...`: il capture overview epoch8
nasce senza live, quindi dichiara `live_control_mode=disabled`. Il successivo
cutover del live aggiorna la modalita' a `persistent_encoder_epoch`, ma non
`childControlSupported`, che rimane false. Il broker prende il ramo statico:
live attivo senza spettatori, limiti termici su FPS/bitrate/analisi ignorati e
calcolo opzionale ancora ammesso. Lo snapshot di rollback non comprendeva
il supporto al controllo.

Alle 13:54 il governor chiedeva live15/1843, analisi10 e optional=false,
mentre lo stato effettivo manteneva live30/4096, analisi20 e optional=true.
Il difetto e' stato riprodotto localmente prima della correzione. Non prova
che questo sia l'unico responsabile del picco termico a circa 95 C.

## Correzione

- Il cutover riconcilia atomicamente modalita' attestata e supporto effettivo
  al controllo, subordinato alla disponibilita' del canale del worker.
- Il vecchio owner perde il supporto live quando il ruolo viene rimosso;
  un percorso passthrough statico non viene spacciato per controllabile.
- Il broker invia immediatamente i controlli aggiornati: non attende un nuovo
  cambio del governor o una connessione del primo spettatore.
- Una domanda live temporanea mantiene soltanto il frame necessario alla
  verifica post-cutover. Anche questa domanda rispetta i limiti termici.
  La verifica riuscita la rilascia e invia il controllo aggiornato: senza
  spettatori l'encoder viene disattivato; con spettatori resta attivo.
- Rollback ripristina supporto e domanda precedenti e reinvia i controlli
  secondo il governor corrente. Coperti anche timeout della verifica e
  rollback dopo verifica completata.
- La verifica rifiuta modalita'/supporto incoerenti con l'attestazione worker.

La logica e' nel coordinatore generico del broker: nessuna eccezione per
test board, tipo di camera, codec, piattaforma o acceleratore. Non vengono cambiati
driver, soglie termiche, profili centrali, percorsi software fallback o
contratti del worker. Nessun nuovo polling nel percorso dei frame.

## Prove locali

`worker_fanout_live_control_test.go` copre modalita' dinamica/statica,
trasporto assente, ruolo rimosso, nominal/thermal_soft/thermal_hard, viewer
presente/assente, ultimo viewer disconnesso e rollback da quattro boundary.
La riproduzione sul codice precedente falliva su supporto obsoleto, cap
ignorati, assenza dei controlli in coda ed encoder sempre attivo.

Suite Go completa su 12 package con `-race -count=1` verde; test mirati
ripetuti con race detector. Verificati anche i contratti nativi di encoder
persistente/protocollo fan-out, transazioni HBN e warmup/control V4L2.
Queste prove host non sostituiscono la misura della disattivazione VPU reale.

## Confine di rilascio

Nessun hotfix manuale eseguito. L'artefatto inizialmente preparato
`c537746f...` NON contiene questa correzione: non va promosso come release
risolutiva di questo difetto. Serve un successore firmato consolidato,
qualificato prima su test board con preflight termico valido, poi eventualmente
promosso identico su deployed board.

Il 2026-09-05 alle 17:14 Europe/Rome e' stato avviato il deploy ufficiale
del successore consolidato `30b83ce8...`, sequenza `1788620948750098790`,
source `f07582d...`. I 133 contratti packaging passano; verifica firma/ZIP e
preflight test board sotto 90 C superati. L'avvio del deploy non e' prova di
installazione conclusa o di corretta disattivazione dell'encoder fisico.

Verifica HW richiesta: capture conservato senza live -> aggiunta live;
controllo attivo attestato; verifica dello switch conclusa; senza viewer
encoder inattivo e analisi/archive ancora freschi; attach/detach e rollback;
cap termici effettivamente applicati senza provocare intenzionalmente
surriscaldamento. Restano separate la qualificazione dello switch e la
diagnosi completa della temperatura.

Evidenze diagnostiche in `/tmp/nemesi-test-board-thermal-audit.636mjtsG/`;
log della correzione in `/tmp/nemesi-live-control-fix.h0fZaeBk/`.

## Prova HW del successore e secondo difetto

Deploy ufficiale `30b83ce8...` concluso il 2026-09-05 verso le 18:02
Europe/Rome. Misura esatta verde, release healthy/applied, token e preset
centrale preservati. Agent PID 1309688, invocation
`f3b11a026e90487a963ac922737fecda`, NRestarts=0.

A piano 55 OOO, senza viewer, encoder live VPU 1280x720 con contatori input
e output entrambi zero; encoder archive 640x360 regolarmente in avanzamento.
OOO -> FOO riuscito in 5,4 s, piano 56: overview PID 1310020/epoch1 conservato,
analisi/archive senza cambi di generation, drop o errori tracker.

FOO -> OOO senza viewer fallisce dopo 90 s nella verifica del bootstrap.
Il worker applica realmente i controlli e produce frame live: non e' piu'
il supporto al controllo dimenticato. Il VPU gia' scaldato puo' emettere IDR
senza ripetere VPS/SPS/PPS; `requestMainBootstrapRecoveryLocked` richiedeva
un client esterno e ignorava `routingLiveDemand`. La verifica non riceveva
quindi la richiesta bounded di rigenerare header + IDR.

Rollback automatico a piano 56 FOO; agent e overview restano invariati.
La frontale, disattivata durante l'attesa, incontra il watchdog e riparte come
PID1323588/epoch2. Nessun riavvio dell'agent o reset USB richiesto manualmente.
MP4 incremental inizialmente idle con raw in scrittura, poi writing con 814
scritture video nel campione post-rollback; non dichiarare perdita dati sulla
sola base del contatore MP4 inizialmente zero.

Secondo fix locale: includere la domanda transitoria dello switch nella
richiesta di bootstrap. Restano invariati rate limit, encoder epoch e cap
del governor; a domanda rilasciata e senza viewer non resta lavoro attivo.
Tre regressioni fan-out falliscono prima del fix (nominal/soft/hard).
Aggiunta prova H265 con IDR senza header, ripetizione limitata, header
successivi e rilascio della domanda, senza simulare un client esterno.

Coperto anche il watchdog del capture intenzionalmente privo di delivery:
durante il ritiro dell'ultimo ruolo o con live-only controllato senza domanda,
zero frame non e' un fault. Il gate usa i medesimi ruoli/cap effettivi gia'
usati dalla misura della cadenza. Admission e flussi richiesti mantengono
esattamente le deadline precedenti. Due test falliscono sul codice precedente
e verificano sia il park sia il ritorno della protezione quando riprende la
domanda. Nessun timeout aumentato, driver modificato o reset USB aggiunto.
Suite Go completa con race detector ripetuta: 12 package verdi, broker
25,430 s; `go vet` verde. I test bootstrap/fan-out ripetuti 20 volte passano.

Il log recorder conferma che l'MP4 inizialmente idle era dovuto al segmento
avviato in ritardo: raw preservati e recuperati, nuovo MP4 dalle 18:10.
Resta separata la latenza storage: checkpoint sync 8616 ms contro soglia severe
3000 ms. Non abbassare i gate per far passare test board.

Non promuovere `30b83ce8...` su deployed board: occorre un successore con questo
secondo fix, poi ripetere il boundary senza viewer, due client e soak.
Evidenze: `/tmp/nemesi-test-board-live-control-release.NYx1nVkk/`, in particolare
`overview-progress2.jsonl`, `switch-events.log`, `after-rollback-sample.jsonl`,
`bootstrap-routing-red.log` e i log dei test green/race.

Il completamento locale e' nel commit `4ccf4db`. Il successore passa di nuovo
tutti i 133 contratti packaging, ma non viene firmato: il runner termina
prima di READY e l'HTTPS verso GitHub/API GitHub va in timeout dal PC.
Nessuna nuova sequenza allocata, nessun secondo deploy. Riprendere dal
packaging ufficiale quando la rete e' ripristinata, senza usare escape hatch
di firma o riutilizzare il vecchio ZIP. Log in
`/tmp/nemesi-package.xyhPyWA8/prepare.log`.

Alle 19:09 dello stesso giorno GitHub/API tornano raggiungibili e l'accesso
autenticato riesce senza modifiche di rete. Nuovo packaging ufficiale:
133 contratti verdi, workflow centrale `33980337987` riuscito, sequenza
`1788628481493817398`, ZIP `cd0d4874...` con byte sorgente verificati.
Controller deploy test board avviato alle 19:15 con il preset centrale invariato;
non equivale ancora a installazione o prova HW completata. deployed board invariato.
Log: `/tmp/nemesi-test-board-live-control-release.NYx1nVkk/successor-cd0d4874-deploy.log`.

## 21:17-21:40: verifica cd0d4874 e bootstrap hardware encoder-owned

Il controller ufficiale termina exit0; postflight indipendente: 8919 entry,
547834409 byte, healthy/applied, token e preset centrale invariati. Agent
PID1458961/invocation `b6980bdfa72e4a34879d4ec30475f996`, NRestarts0.
Il campione completo fallisce sull'audio recorder fermo: ALSA/kernel attesta
`Not enough bandwidth`, `usb_set_interface failed (-28)` sul microfono
C-Media 1-1.2, che condivide il ramo USB2 con le due camere. Video recorder
e tracker avanzano. Non e' un motivo per disabilitare audio o allentare il soak.

Tentativo di recovery verso OOO usando il controllo autenticato, CAS e il
piano56 FOO esatto: operation `cd0d-usb-audio-recovery-apply-20260905`.
Non considerato preflight/test superato: preservata separatamente la prova
rossa audio. Lo switch fallisce ancora dopo90s in verify, rollback automatico.
Questa volta i log attestano richieste `hardware_headers_plus_idr` ogni2s e
risposte control-ready del worker: il difetto e' nel confine successivo.
Il candidate ha consumato il primo VPS/SPS/PPS+IDR durante warmup; la successiva
richiesta vendor puo' restituire soltanto IDR. Il broker non ha mai visto i
parametri, quindi non puo' conservarli da un epoch precedente.

Correzione in `native/h26x_bootstrap.h`: cache bounded64KiB dell'intera famiglia
di parametri realmente emessa dall'encoder, senza GOP/frame storici. Il percorso
V4L2 persistent e il percorso HBN/SP la possiedono e la mantengono nel medesimo
encoder attraverso warmup/cutover/rate control/demand; nuovo encoder o famiglia
parziale invalidano la configurazione precedente. Prefisso ai soli IDR privi
di parametri, nessuna nuova allocazione o ricreazione VPU nel percorso dei frame.
H264 e H265, live e archive condividono l'helper; nessuna dipendenza Nemesi.

Regressione host red sul codice precedente, green dopo il fix. Coperti due
codec, configurazione completa/parziale/sostituita, frame inter invariati,
buffer insufficiente, parametro oversize, rilascio del buffer vendor anche
in errore, epoch e nuova istanza. ASAN/UBSAN/LeakSanitizer verdi. Il fake HBN
ora emette parametri soltanto nel primo frame privato: ogni IDR pubblicato
deve avere il bootstrap completo, anche dopo cutover e cambio codec; verde.
Suite Go12package race e vet verdi (broker25,472s).

Sonda hardware separata dalla release, compilata in directory temporanea con
lo stesso codice e libmultimedia verificata: `RDKX5_PERSISTENT_ENCODER_BOOTSTRAP_OK`.
VPU reale emette32/33/34/19 alla prima attivazione, dopo warmup privato, su
recovery esplicita e resume a epoch2. Rate medium/low conservano epoch1 e
bitrate3276/2457 con frame freschi: non si impone un IDR immediato durante
una variazione continua, mentre i nuovi boundary decoder lo richiedono.
Prima sonda con quest'ultimo vincolo improprio documentata come fallita,
non cancellata. Post-scan: solo l'encoder archive resta allocato, agent
invariato, temperature circa81C. Nessun hotfix sotto /opt/nemesi.

Restano da fare: nuovo ZIP ufficiale, switch OOO e verifica audio recuperato,
park/resume con contatori VPU e due consumer, soak canonico. La sonda VPU
non certifica da sola il coordinatore o l'hardware HBN fisico; deployed board invariato.
Log sotto lo stesso root audit: `cd0d-native-bootstrap-red.log`,
`cd0d-native-generic-sanitizer.log`, `cd0d-hbn-final.log`,
`cd0d-native-hw-probe-final.log`, `cd0d-audio-kernel-and-alsa.log`.

## Chiusura del boundary specifico su test board, 2026-09-06

Il successore ufficiale `1ecd1414...` include tutti i correttivi sopra e il
clock pubblico persistente. Tre switch a zero viewer e tre cicli di due
consumer90/90/75s passano; VPU live fermo in idle e archive sempre in
avanzamento. Segue soak canonico2700,118s/5401campioni, interamente coperto
da due consumer2850s: `passed`, massimo83,447C, stesso agent/NRestarts0,
zero nuovi drop/errori recorder. Dopo la chiusura dei client, ulteriore prova
HW16,538s con live input/output+0 e archive+248; audio e analisi proseguono.

Questa e' prova fisica del park/resume dopo switch e sostituzione encoder,
non una prova provocata a90/95C. Nominal/soft/hard e rollback restano coperti
dai test causali del coordinatore; i cambi rate/bitrate VPU dalla sonda
separata riportata sopra. Non si forza il surriscaldamento e non si dichiara
risolta ogni possibile causa termica o di latenza storage. I30fps nominali
della panoramica restano circa25fps misurati, nella relativa issue.

Frontale ripristinata con CAS alla rev64FOO senza riavvii, analysis/archive
preservati. Misura esatta finale, token e preset centrale invariati.
Report e dettagli puntuali nella issue generazioni live; nessun nuovo gate
browser considerato superato e nessun deploy deployed board effettuato per questo ZIP.
