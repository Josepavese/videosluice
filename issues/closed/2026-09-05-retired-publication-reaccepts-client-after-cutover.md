# Client accettato dal vecchio socket dopo il cutover

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. L'attach rifiuta publication ritirate sotto lo stesso lock del cutover, per tutti i ruoli.

Evidenza: 15 regressioni causali, fence incluso nei successori, matrice e browser switch a571 senza refresh; soak test board verde.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


Stato: correzione locale e regressioni verdi; non ancora distribuita.

## Evidenza reale test board

Il 2026-09-05 la verifica a due client (webserver desktop e webapp cloud con
viewport mobile) sulla release `fe23695d...` esegue dal menu:

- rev. 52 `FOO` -> rev. 53 `OOO`, cambiando solo live front -> overview;
- rev. 53 -> rev. 54 `FOO`, riattivando front per live e lasciando analysis e
  archive a overview. ApplyGraph attesta successo alle 11:41:09 Europe/Rome.

Dopo il secondo cambio, overview conserva correttamente i soli ruoli
`-/analysis/archive`, ma riceve recovery ripetute per
`main_no_frames_for_20.0s_watchdog_17.0s`. Nei receipt di fine epoch ci sono
`frames_main=0`, circa 225 frame archive e 299 analysis, con age di questi
ultimi circa 130-175 ms: la recovery viene richiesta per un ruolo rimosso,
non per assenza di tutti i frame. I browser successivamente restano con ICE
connesso ma senza nuovi frame video; il gate tracker respinge correttamente
una freshness di circa 7 secondi. Il test non e` passed.

Questo riscontro non annulla il soak storico positivo della release: espone
un'interleaving aggiuntiva durante switch con client connessi. Non e` prova
di SD difettosa, leak hardware o difetto specifico del player pubblico.

## Causa e correzione

`acceptLoop` poteva ottenere una connessione dal listener precedente e
attendere `runtime.mu` mentre il cutover rimuoveva i client e cambiava ruolo,
piano e pubblicazione. `attachClient` non verificava ne` cancellazione del
contesto ne` identita` della pubblicazione: poteva registrare il client dopo
il distacco, ricreando domanda live sul vecchio owner.

Ora ogni attach porta la pubblicazione che ha accettato la connessione e,
sotto lo stesso lock del cutover, verifica prima di mutare qualsiasi stato:

1. contesto dell'accept ancora attivo;
2. ruolo ancora abilitato;
3. stessa pubblicazione attuale del ruolo;
4. ownership dell'inode sul percorso logico corrente.

Il controllo riguarda live, archive e analysis e non dipende da codec,
piattaforma o acceleratore. Non viene indebolito il watchdog e non vengono
nascosti client obsoleti dietro una semplice esclusione del ruolo dal gate.

Quindici casi (tre ruoli per cinque boundary: rimozione, disabilitazione,
sostituzione della pubblicazione, perdita inode, cancellazione) falliscono
sull'implementazione precedente con client indebitamente ammessi e passano
con la correzione. Il test bootstrap continua a provare che una scrittura
lenta non trattiene il lock globale. Suite Go completa `-race -count=1` e
`go vet ./...` verdi.

## Recovery e confine di rilascio

Alle 11:47:04 il rollback applicativo ufficiale termina `succeeded`, rev. 55
`OOO`. La release non cambia, l'agent conserva PID `158136`, invocation e
zero restart. Overview PID `851677`, epoch 8, e` invariata tra le due letture
successive fino alle 11:52:23; tracker avanza e ha age 60 ms, recorder avanza,
finalizzazioni 45 -> 46, zero drop/errori archive. Il video locale e` stato
nuovamente verificato visivamente. Nessun reset o riavvio manuale del device.

test board resta temporaneamente con live sulla panoramica, non sulla frontale;
il piano `FOO` va riprovato solo dopo la correzione qualificata. deployed board non e`
stato modificato: nessuna conclusione di immunita` del suo codice deriva dal
fatto che il suo profilo corrente e` single-source.

Prima del successore consolidato resta da correggere anche la
[conferma UI durante refresh](../../../webserver/issues/2026-09-05-video-routing-confirmation-lost-during-refresh.md).
Poi: workflow firmato ufficiale su test board, riproduzione del boundary con client
connessi e verifica di endpoint/owner/frame peer oltre la finestra watchdog,
soak canonico e solo successivamente valutazione della promozione su deployed board.
Nessun hotfix manuale e` stato applicato alla release misurata.

Evidenze temporanee sotto `/tmp/nemesi-test-board-fe236-qualification.es1ynLwp/`:
`test-board-two-clients-*`, `test-board-browser-multiclient-*`,
`test-board-after-browser-journal.log`, `test-board-after-browser-closed.stderr`,
`test-board-recovery-to-overview.json`, `test-board-recovered-overview-{first,final}.jsonl`,
`test-board-stale-publication-{red,green,full-race,vet}.log`.
La schermata cloud mobile nera non e` stata dichiarata verde usando i soli
contatori: canvas mostra pixel non neri e nessun overlay DOM davanti al video,
ma questo non prova la composizione visiva. Nessuna modifica al player cloud.
