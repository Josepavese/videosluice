# Una sorgente esplicita viene rifiutata per FPS bassi anche se produce video valido

- Stato: distribuita su test board e deployed board; ammissione/switch a FPS ridotti e soak di entrambi verificati; browser del caso lento ancora distinto e aperto
- Severita`: alta; impedisce di usare la camera richiesta dall'operatore
- Rilevata: 2026-09-05
- Componenti: video-broker, admission, ApplyGraph, stato runtime, integrazione Impostazioni
- Baseline esaminata: `3288eb0` (`fix(ipc): keep durable consumers draining`)
- Correlata: [menu camere svuotato dopo timeout](../../webserver/issues/2026-09-05-video-routing-menu-disappears-on-timeout.md)

## Requisito funzionale dell'operatore

Una sorgente assegnata a uno stream identifica il contenuto da acquisire.
Camere diverse possono avere compiti, inquadrature, ottiche e posizioni diverse:
per esempio una camera frontale per la diretta e una panoramica per analisi e
registrazione. La seconda non puo` essere considerata un sostituto equivalente
della prima soltanto perche` raggiunge una frequenza maggiore.

Il requisito vale con una, due o piu` sorgenti e per ogni stream configurabile,
senza associare al broker significati applicativi rigidi come "frontale sempre
live". I ruoli reali sono definiti dai binding espliciti dell'operatore.

**Se la live deve usare una determinata camera, deve poterla usare anche sotto
gli FPS desiderati, finche` il percorso produce frame validi e utilizzabili.**
Lo scostamento di prestazione deve essere visibile; non deve diventare da solo
un rifiuto della sorgente, una sostituzione di camera o un ciclo di riavvii.

## Problema osservato su test board

Il 2026-09-05 l'operatore ha richiesto dal pannello il cambio della live da
`overview` a `front`. La revisione corrente era `36`.

La frontale era configurata H265 1280x720@25 e il bridge rilevava:

```text
status=rejected codec=h265 reason=runtime_attestation_timeout
frames=108 observed_fps=10.80 expected_fps=25 effective_fps=11
vps=true sps=true pps=true keyframe=true
```

La verifica iniziale ha rifiutato ripetutamente la sorgente, incrementando i
fallimenti del modo e attivando quarantena/backoff. L'operazione
`web-1788560696317341942-3`, iniziata alle 00:24:56 Europe/Rome, e` terminata
alle 00:26:26 con `state=failed`, `phase=abort`, errore `apply_failed` nella
fase `admission`.

La revisione e i binding sono rimasti invariati: live, analysis e archive su
`overview`. Questo episodio e` un cambio richiesto e rifiutato prima del
cutover, non la prova di un cambio automatico verso una camera arbitraria.
L'effetto per l'operatore resta inaccettabile: la camera voluta produce video,
ma il sistema non gli permette di assegnarla alla live per il solo limite FPS.

La causa fisica della riduzione degli FPS non e` accertata da questi log.
Scarsa luce, esposizione, driver, carico o trasporto richiedono evidenze
distinte; non dedurre una policy globale da un singolo modello di camera.

## Punti confermati nel codice

- `native/stable_cadence.h`, `video_broker_stable_cadence_meets_expected()`:
  l'ammissione richiede almeno l'80% della frequenza attesa.
- `native/rdkx5_v4l2_mux_bridge.c`: la finestra
  `kCameraAdmissionTimeoutUs` e` di 10 secondi. Se la frequenza non soddisfa
  l'ammissione, il percorso emette `runtime_attestation_timeout` e termina
  tramite `cleanup`, anche in presenza di frame decodificati e bootstrap.
- `internal/broker/camera_admission.go`: l'ammissione Go combina contratto,
  bootstrap e frequenza. La conservazione del produttore degradato non basta
  se il bridge nativo termina prima o non espone l'evidenza necessaria.
- `internal/broker/broker.go`: a regime la bassa frequenza viene gia` segnalata
  e il produttore viene conservato dal watchdog di cadence. La soglia e` il
  55% del target effettivo, con minimo 2 fps. Non equivale a una segnalazione
  precisa di ogni scostamento dal profilo richiesto.
- `internal/broker/routing_runtime.go`,
  `validateLiveProfileCandidateLocked()`: un'ammissione incompleta impedisce
  l'applicazione; esiste inoltre un confronto fra FPS candidati e
  `observedCaptureFPS`. Va distinta la semantica di questo valore dalla
  misura temporale dei frame realmente prodotti.

Abbassare soltanto il limite FPS dell'uscita live non risolve necessariamente
il problema: il vincolo che rifiuta la camera puo` appartenere alla cattura.

## Contratto da realizzare

Separare disponibilita`, prestazione e scelta di sorgente:

| Condizione | Comportamento richiesto |
| --- | --- |
| Camera selezionata, frame validi, FPS inferiori al desiderato | Avviare o mantenere quella camera; segnalare la prestazione ridotta |
| Cambio esplicito verso una camera valida ma lenta | Completare il cambio transazionale; confermare la nuova sorgente e mostrare l'avviso FPS |
| Frequenza che torna al livello atteso | Rimuovere l'avviso dopo stabilizzazione, senza cambiare sorgente o riavviare |
| Nessun frame utilizzabile oltre il limite di stallo previsto | Segnalare indisponibilita` e applicare il recupero limitato sulla stessa sorgente |
| Frame indecodificabili, bootstrap assente, risorsa hardware non disponibile o vincolo hardware obbligatorio violato | Mantenere i gate strutturali; esporre la causa reale |
| Errore reale durante una transazione | Abort/rollback attestato; mostrare che la nuova scelta non e` stata applicata e quale sorgente resta effettivamente attiva |

Gli FPS desiderati sono un obiettivo di prestazione, non un'autorizzazione a
scegliere un'altra inquadratura. "Indipendentemente dagli FPS" non significa
considerare vivo un flusso fermo, mostrare indefinitamente l'ultimo fotogramma,
accettare dati corrotti o rimuovere protezioni termiche e di piattaforma.

Restano distinti almeno questi dati, con nomi da definire nel contratto
current-only:

- sorgente richiesta e sorgente effettivamente confermata;
- profilo richiesto e modo di cattura negoziato con il dispositivo;
- FPS desiderati dall'operatore;
- target effettivo dopo eventuale adattamento esplicito di rete/risorse;
- FPS realmente misurati su una finestra definita;
- disponibilita` del video e avviso di prestazione con relativa causa.

Un adattamento legittimo della live per congestione non deve essere confuso
con sotto-produzione della camera. Entrambi devono restare leggibili senza
riscrivere silenziosamente la preferenza persistente dell'operatore.

## Percorso di correzione

1. **Definire prima il contratto generico.** Distinguere prove strutturali
   obbligatorie da obiettivi FPS. Conservare source identity, formato realmente
   supportato, geometria, bootstrap codec e acceleratori richiesti. Una camera
   lenta non deve essere dichiarata capace di un modo che non supporta.
2. **Allineare tutte le fasi e tutti i backend.** Analizzare admission iniziale,
   ripartenza, prepare/warmup, primo frame, cutover, verifica e stato a regime.
   Applicare la stessa semantica ai backend supportati e ai percorsi VPU, JPU,
   GPU/OpenCL, scaler e BPU/NPU ove coinvolti; lasciare specifica del backend
   la prova dell'uso hardware. Non basta rimuovere il timeout C o abbassare
   una soglia Go.
3. **Separare sotto-produzione e recupero da guasto.** FPS bassi con frame
   freschi devono produrre un avviso e consentire progressione. Non devono
   incrementare quarantena del modo o backoff, provocare reset UVC o cambiare
   camera. I veri stalli e gli errori di decode mantengono limiti di recupero
   finiti. Le finestre di avvio e stallo devono riconoscere un flusso lento
   funzionante senza attendere arbitrariamente 30 frame o il target nominale.
4. **Integrare ApplyGraph e il pannello.** Una destinazione lenta ma valida
   deve poter superare admission e diventare la sorgente confermata.
   Conservare un solo capture owner, bootstrap H265 VPS/SPS/PPS+IDR e cutover
   atomico. Gli stream non coinvolti devono mantenere owner, generazione e
   continuita`. Mostrare per esempio: "Frontale attiva: 10,8 fps su 25
   richiesti". Il menu deve restare recuperabile secondo la issue correlata.
5. **Rivedere i gate e la documentazione.** Distinguere il funzionamento
   corretto a FPS ridotti dalla qualificazione delle prestazioni richieste.
   Non marcare un profilo 25/30 fps come qualificato soltanto perche` la live
   e` visibile a 10 fps. Misurare separatamente continuita`, latenza, risorse
   e scostamento FPS; preservare durata e rigore del soak. Gli avvisi di
   prestazione non devono innescare recovery/deploy automatici impropri.

La precedente [issue sulla sorgente non disponibile](closed/2026-09-03-routing-cannot-evacuate-unavailable-source.md)
descriveva come corretto il rifiuto per il contratto FPS allora vigente.
Questa richiesta modifica quel requisito funzionale. In fase di implementazione
allineare i riferimenti operativi preservando la cronologia dell'incidente.
Conservare inoltre la distinzione gia` introdotta dalla
[correzione dell'adattamento live](closed/2026-09-03-runtime-live-adaptation-falsely-degrades-source.md).

## Test e criteri di accettazione

- Riprodurre deterministicamente una sorgente con modo nominale 25/30 fps che
  consegna frame validi a 10 fps: avvio riuscito, sorgente corretta, avviso
  accurato, nessun riavvio o incremento della quarantena per i soli FPS.
- Ripetere con calo dopo l'avvio e successivo recupero; coprire oscillazioni
  e brevi buchi con isteresi degli avvisi e log limitati.
- Coprire frequenze molto basse ma supportate, vero stop dei frame, immagini
  corrotte, bootstrap assente e indisponibilita` dell'acceleratore. Non
  mascherare queste condizioni come semplice lentezza.
- Richiedere esplicitamente il cambio da una camera veloce a una lenta:
  verificare il contenuto della nuova camera, revisione persistita, bootstrap,
  epoch e generazioni; non e` sufficiente osservare video da una sorgente
  qualsiasi. Nessun fallback software quando il profilo lo vieta.
- Verificare il caso multi-camera con compiti distinti: cambiare soltanto live
  non deve spostare analysis/tracker o archive/recorder. Estendere la matrice a
  1, 2, 3 e almeno 5 sorgenti, senza indici speciali primaria/secondaria.
- Coprire condivisione della cattura, client multipli e target indipendenti:
  l'avviso deve appartenere al percorso interessato e non duplicare il capture
  owner o creare code per simulare gli FPS richiesti. Non duplicare frame
  per falsare la misura.
- Coprire restart, persistenza, cancel, concorrenza e rollback per guasto
  reale. Un timeout di lettura non deve ripetere automaticamente la mutazione.
- Verificare webserver e webapp con la sorgente lenta: immagine corretta,
  ritardo contenuto, avviso visibile e nessuna scomparsa dei controlli.
- Qualificare su test board prima di una promozione ufficiale A/B su deployed board. Usare
  test controllati per i casi FPS, senza cambiare automaticamente esposizione,
  profili centrali o soglie del soak. Conservare le evidenze di prestazione
  mancata come tali.

## Operazioni escluse da questa issue documentale

La creazione del documento non modifica codice, configurazioni, binding,
autoesposizione, servizi o dispositivi. L'implementazione deve restare generica
e standalone, senza eccezioni per test board, per un identificativo USB o per Nemesi.

## Implementazione e prove locali del 2026-09-05

La successiva implementazione sostituisce l'ammissione basata sulla percentuale
FPS con tre frame freschi per ruolo e bootstrap completo. Conserva il controllo
del modo negoziato e tutte le attestazioni hardware. La cadenza nativa diventa
una misura scorrevole, aggiornata anche dopo l'ammissione. Il circuit breaker
v10 invalida le precedenti classificazioni che potevano includere soli FPS bassi.

Lo stato `performance` di ogni stream distingue richiesta persistita, target
effettivo, misura e finestra temporale. L'avviso compare dopo uno scostamento
sostenuto e si risolve con isteresi; la disponibilita` strutturale resta separata.
La proiezione routing lega la misura a sorgente e generazione confermate. Il
pannello mostra la fluidita` misurata e gli eventuali adattamenti senza cambiare
la bozza o il binding. La diagnostica qualita` dei worker V4L2/HBN viene inoltre
pubblicata su tempo trascorso anche quando il conteggio frame avanza lentamente;
il percorso GStreamer usa gia` campionamento temporale senza duplicare frame.

Prove completate prima del deploy: suite Go completa del prodotto anche copiato
fuori da Nemesi, race su broker/controllo/routing, test webserver e 52 test browser
JavaScript, ASan/UBSan della cadenza nativa, fan-out HBN compilato con runtime di
test, contratti V4L2 e soak. I casi deterministici includono 25/30 richiesti con
10 e 1 fps effettivi, recupero, isteresi, frame scaduti e gate hardware ancora
obbligatori. La proiezione runtime copre 1/2/3/5 sorgenti.

Nel browser locale con risposte simulate sono stati verificati avviso 10,8/25,
errore HTTP 503 con controlli conservati e disabilitati, successivo recupero
automatico e cinque sorgenti; il layout scuro e` stato ispezionato su desktop
e larghezza mobile. Queste prove non certificano il video o gli acceleratori
dei dispositivi. Restano deploy A/B test board, switch reale alla camera lenta,
permutazioni e soak >=2700 s, poi la promozione deployed board. Nessun nuovo artefatto
e` ancora stato distribuito per questa correzione.

Il primo packaging di `da5a133` ha intercettato il postflight non ancora
allineato a `performance` e alla proiezione routing `runtime`. Il validatore
ora verifica entrambi con campi current-only, binding e misure coerenti;
regressioni confrontano il produttore Go e la semantica del soak, oltre a
risposte reali su socket UNIX autenticati. Il contratto completo di postflight
e` verde. Nessuna soglia, durata soak o attestazione hardware e` stata ridotta.

La candidata `f3ec414`, artefatto firmato `6fd73a47486b542b...`, e` stata poi
distribuita su test board tramite A/B ufficiale con trial e postflight verdi. La
verifica indipendente conferma release esatta e piano operatore rev. 36
preservato. Questo chiude il passaggio di deploy, non il difetto osservato
sulla frontale: switch reale alla sorgente lenta, matrice/browser e soak
restano necessari prima di chiudere questa issue e promuovere su deployed board.

Il successivo test reale delle 02:56 Europe/Rome ha applicato `OOO -> FOO`
in 13,082 s, revisione 37: la frontale e` ammessa e resta selezionata a circa
10,8-11 fps su 25 richiesti, con `performance.state=reduced`. Analysis e
archive overview hanno conservato PID capture, media epoch, generazioni e
contatori senza nuovi drop/disconnessioni. Un consumer archive continuo per
120 s ha attraversato lo switch a 15,017 fps, gap massimo 95,1 ms, socket
invariato. Il consumer live ha verificato VPS/SPS/PPS+IDR. Una seconda misura
di 15 s separa bootstrap e regime: eta` dei pacchetti dal timestamp broker
p95 1,195 ms e massimo 2,845 ms dopo il primo secondo. Non e` una misura
camera-to-display o una verifica browser.

La matrice seguente ha pero` esposto una [race nel marcatore di nuova
pubblicazione](closed/2026-09-05-first-queued-packet-misses-publication-reset.md),
che deve essere corretta e riqualificata prima del soak e di deployed board. Il fatto
che la sorgente lenta sia ora utilizzabile non qualifica il target 25 fps.

## Qualificazione successiva e promozione deployed board

La race descritta sopra e` stata corretta nel successore firmato `fe23695d...`:
otto transazioni test board (rev. 42 -> 50), soak canonico di 2700 s, consumer
simultanei e recupero source-local sono verdi. Le evidenze di basso FPS restano
valide; la successiva osservazione diurna a 25 fps non prova la causa fisica
della riduzione notturna ne` una frequenza sempre garantita.

Il 2026-09-05 l'utente ha autorizzato deployed board sulla qualificazione raccolta e ha
chiesto di non concentrare altro lavoro su una matrice pesante del player
pubblico. Lo stesso artefatto e` ora `healthy`/`applied` anche su deployed board, con
profilo centrale v4 e nessun nuovo rebuild. Il webserver deployed board ha mostrato
H265 1280x720@20 e il catalogo single-source corretto; non e` una prova browser
del caso lento test board. Il soak deployed board di 2700 s e` terminato `passed` alle
11:13:23, report `816fb48668cfa368eff73abf34c6d8f0`, 5400 campioni e release
confermata anche dopo il controllo automatico del canale precedente.
Stato corrente e prove sono nel
[storico del goal](../../../improvements/HISTORY_VIDEO_BROKER_V3_2026-09-06.md).
