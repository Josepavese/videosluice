# FAP: la contabilizzazione runtime puo' bloccare la ricezione dei frame

Stato: **aperto, bloccante per il sotto-rilascio test board -> deployed board**.
Ambito: video-broker standalone; nessuna modifica a profili, deadline,
integrita', identita' causali o soglie di qualifica per aggirare il problema.

## Evidenza sul dispositivo

13settembre2026, candidato firmato641188be, trial test board:

- Python, compilazione e verifica Event-AI BPU/ONNX PASS.
- Alle04:20:40UTC il primo output analysis, origine147, e' interamente
  ricevuto:23record,345600byte. Il budget totale di999969us e' superato
  (1095654us) nella fase publication.
- Il commit atomico riporta3979us; `observeCausalDelivery` attende303125us
  su `rt.mu`, con207614us residui. La verifica della deadline dopo il lock
  rifiuta correttamente l'aggiornamento tardivo dello stato e ritira il reader.
- Il successivo output nativo, origine148, scade dopo1000ms/49020byte:
  conseguenza, non prima causa. Il binding ritirato fa uscire Tracker e
  il gate rifiuta `task 6 reported exits=1`.

Il sampler indipendente non mostra pausa globale del suo scheduling:
massimo intervallo0,508s su101campioni nella finestra04:20:15..04:21:10.
MemAvailable minimo1861MiB, temperatura massima69,817C. Nei campioni vicini
al guasto CPU9,4/7,7% e iowait51,9/56,6%, fino a9processi bloccati. Questo
dimostra pressione I/O concomitante, non l'owner del lock, un difetto fisico
della SD, un leak HW o un problema della camera. PSI non esposta dal kernel.

Il salto H265 del trial precedente e' una prova distinta; non attribuirgli
automaticamente questo errore o dichiararlo risolto.

La fase publication misura864661us, mentre il callback diagnostico misura
307198us (commit3979us, observer303217us): restano circa557ms prima/intorno
al callback da attribuire. Il solo lock non descrive tutto il costo rilevato.
Publish/Snapshot della delivery gia' usano percorsi indipendenti: non e'
dimostrato un blocco diretto del produttore causato dal reader Tracker.

## Recovery e limite ancora aperto

Il recovery automatico ha ripristinato fc908a ma il suo bootstrap nativo
e' scaduto dopo5001ms prima dell'autenticazione del peer. Non potendo
confermare il rilascio SDK, il broker ha mantenuto la sorgente in quarantena;
la verifica di salute e' scaduta dopo900s e ha fermato il servizio. Nessuna
causa fisica o specifica della camera e' provata da questa evidenza.

Alle04:45:57UTC, con transazione terminale, runtime fermo, misura esatta e
configurazioni verificate, il servizio canonico di riconciliazione ha
riconosciuto `recovered_after_interruption` e ritirato l'inhibitor. Un solo
avvio controllato ha ripristinato il servizio: postflight canonico15s e
accesso runtime PASS04:48:27, Tracker epoch1/nessuna riapertura o errore,
recorder in avanzamento. Non e' la qualifica del nuovo candidato e non
dimostra risolto il caso di bootstrap a freddo. Non alzare i timeout per
nascondere il problema. Campioni finali preservati e osservatore fermato.

## Correttiva e criteri

Correttiva implementata13settembre e verificata in sviluppo su test board,
**non ancora qualificata come rilascio firmato**:
il callback causale accumula soltanto metadati in una struttura RAM fissa
per epoch; un solo worker, collegato e riunito al ciclo di vita del receiver,
esegue accounting/admission/logging senza bloccare la ricezione. Nessun pixel
o contesto per-frame viene conservato. Conteggi, gap e ultima finestra
contigua restano esatti; si conservano i tempi di ricezione, non quelli della
successiva contabilizzazione. Cancellazione, sostituzione dell'owner, epoch,
source e gate analysis scartano le evidenze non piu' applicabili.

Regressione RED con il callback precedente (lock runtime occupato -> frame
bloccato) e GREEN dopo la separazione. PASS suite broker/ingress/framedomain/
composizione, anche `-race`; prova deterministica10000osservazioni con lock
runtime trattenuto, finestra admission equivalente al percorso sincrono,
identita'/cancellazione/gate e salute stale. Microbenchmark workstation:
64,76ns/osservazione,0B e0alloc; non e' una misura del device o della pipeline.
`output_commit_progress` ora attribuisce separatamente ticket, binding,
validazione prima/dopo e consumer; nessuna modifica a deadline o failure/replay.

Build mirata test board PASS in PAL privato; nativi riusati solo con gli hash
della qualifica OpenCL/JPU gia' valida. Il primo preflight della build si e'
fermato per un documento di issue non incluso nell'elenco delta, senza toccare
la release attiva. Elenco completo verificato prima della ripresa.

Promozione misurata `development_dirty` PASS13settembre05:41UTC, configurazioni
e permessi preservati,9355voci/553682130byte. Postflight canonico15s/accesso
runtime PASS. Prima osservazione interrotta solo dalla sonda CPU per ESRCH
su un processo accessorio terminato:17campioni/80,927s, Tracker19,165FPS e
nessun errore di lettura. Corretto esclusivamente il censimento diagnostico
dei processi accessori; nessun riavvio/deploy o attenuazione dei controlli
sui processi richiesti. Nuova osservazione continua **PASS124,310s**:
Tracker19,339FPS, stessi PID/epoch, zero read_errors/mapping_reopens,
temperatura massima80,222C. CPU media riferita a un core: broker51,67%,
bridge nativo56,09%, Tracker50,26%; RSS broker135,33->135,73MiB,
bridge61,85->61,80MiB. Non e' una prova di leak risolti nel lungo periodo,
di immagine/persona corretta nel nero, ne' il soak del rilascio firmato.
Restano rilascio ufficiale, soak test board e, solo dopo PASS, deployed board/soak.
Evidenze locali: `<private-workspace>`.

1. Attestare il recovery canonico, preservare diagnosi e configurazioni;
   non riprendere o duplicare questa transazione.
2. Individuare il tratto che mantiene il lock condiviso e verificare il
   confine fra ricezione/commit del frame e contabilizzazione runtime.
3. Rendere quel percorso limitato e indipendente dal lavoro lento senza
   nuove code illimitate, per-frame goroutine, frame rinominati, stato sano
   costruito da dati scaduti o indebolimento di admission/fencing/integrita'.
4. Test deterministici di contesa e prova integrata test board con stesso profilo,
   circa30FPS sorgente/20analysis/15archive. La prova deve includere i
   consumer coinvolti, non solo un microbenchmark o l'assenza di errori.
5. Riconciliare la correttiva qualificata, un rilascio ufficiale test board e soak
   canonico>=2700s; soltanto dopo stesso artefatto deployed board e soak. Dopo PASS
   anche deployed board chiudere questa issue e fermarsi come richiesto dall'utente.

Evidenze private sotto
`<private-workspace>`:
`test-board-trial-journal.log` (causa completa in `source_receive_progress`),
`test-board-trial-failure-1.log`, `trial-pressure-snapshot.jsonl`,
`test-board-recovery-progress-1.log`. Il journal va raccolto con identificatore
`nemesi-release-trial`: il solo filtro per unita' omette parte dei log
inoltrati dai componenti. Snapshot diagnostico non sostituisce il risultato
di recovery o soak, entrambi ancora da completare per questo candidato.
