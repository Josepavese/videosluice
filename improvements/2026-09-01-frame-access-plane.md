# Frame Access Plane: accesso puntuale e correlato al video graph

## Stato e decisione

Proposta architetturale del 2026-09-01, **in lavorazione dal 2026-09-06** su
richiesta dell'utente. Stato aggiornato al10settembre: contratti causali,
Frame Access RPC, SDK e crop source-resolution sono implementati e integrati
su test board; prove native pixel/ROI e osservazioni Registry sono documentate nella
[checklist autorevole](../../../improvements/CURRENT_GOAL.md) e nel relativo
handoff. Restano qualifica prolungata, regressione prestazioni e gli altri
criteri del goal: NON chiusa, nessuna qualifica universale o deployed board implicita.
Le sezioni datate seguenti conservano audit e progressi storici; le frasi
"non ancora" descrivono quelle date, non lo stato operativo corrente.

**Scope corrente, decisione utente12settembre:** qualifica di una sorgente
fisica30FPS attraverso il FAP, test board front1920x1080, con ROI/consumer attivi.
La qualifica fisica simultanea di due sorgenti passa APERTA al
[backlog B4a](../../../improvements/GOAL_ACTIVITY_ALLOCATION_2026-09-06.md),
non e' completata o necessaria alla chiusura corrente. Contratti standalone,
isolamento multi-source e fencing rimangono generici. Tracker/analysis circa20FPS,
archive15FPS e profilo deployed board20FPS non cambiano.

Ripresa7settembre dopo consolidamento realmente superato: candidato isolato
`/tmp/nemesi-causal-envelope.4ZV7248G/` con codec in-memory Go/Python VBCF448,
namespace/process fence, epoche source/publication distinte, exact/unavailable,
clock espliciti, geometria/stride/digest e controllo epoch distinto dai frame.
5vettori indipendenti bidirezionali e2299mutazioni concordi, race count3,
6testPython,vet e fuzz460170esecuzioni PASS. Digest non e' autenticazione.
Aggiunti reader parziali Go/Python con timeout/resume, poison, quota/abort/EOF;
race count3/coverage96.4%,12testPython,vet/PAL e fuzzstream451185PASS.
Codec C indipendente verificato su5vettori/4608casi comuni, ASan/UBSan e
fuzzseed1474163PASS (piu' ripetizione bounded918572). Restano
deadline del trasporto, SHM e integrazione worker/mux/broker/SDK/consumer;
nessun reader nuovo o maggiore risoluzione attivati sul device.

Snapshot7settembre: VBSC64Go/Python e validazione di copie immutabili
before/packet/after aggiunti nel candidato;3599casi concordi,5vettori,
racecount3/97.1%,15testPython,vet/PAL e fuzz352635PASS. Generation/namespace,
capacity/used e control stabile verificati senza retry. NON implementa ancora
load atomiche, memory ordering o lifetime del mapping reale: restano provider
SHM/native e integrazione, nessuna correlazione hardware dichiarata per questi test.
Codec snapshot C indipendente aggiunto e concorde sugli stessi3599casi,
ASan/UBSan e warning strict PASS; suite finale17metodi Python e Go race/vet
verdi. Fuzz nativo senza symbolizer completato983868esecuzioni; primo run
con symbolizer scaduto al bound20s, INCOMPLETO e conservato, non contato verde.

Passo successivo7settembre: primitiva nativa di accesso mappato con atomic64
anche per il payload, un tentativo bounded, niente spin/lock del lettore,
overflow/generazioni rifiutati.1.000pubblicazioni pthread e1.000tra processi
reali con mapping sola lettura e memfd sealed; ASan/UBSan/TSan PASS locali.
Non e' ancora il provider completo: distribuzione FD/autenticazione/retirement,
SDK e worker mancanti, ABI ARM64/hardware non qualificati. Riferimenti e
limiti in `/tmp/nemesi-causal-envelope.4ZV7248G/atomic-reference-notes.md`.

Decisione di sviluppo: estendere `video-broker` con un **Frame Access Plane**
pubblico, general-purpose, source-bound e on demand. Non e' un quarto stream e
non e' un'API dedicata a Person Registry. E' un piano di accesso puntuale alle
superfici del media graph, ortogonale agli stream continui `live`, `analysis` e
`archive`.

Person Registry e' il primo caso d'uso: il tracker esegue inferenza su
`analysis`, poi richiede uno o piu' ROI dalla superficie `processed_full` dello
stesso frame origine. La medesima API deve poter servire, senza modifiche al
broker, thumbnail evento, OCR, snapshot diagnostici, validazione qualita' e
altri consumer locali.

La feature appartiene al prodotto `video-broker`. Il broker non deve importare
configurazioni, tipi o semantica applicativa Nemesi; gli adapter applicativi
traducono i propri bisogni nel contratto pubblico del prodotto.

## Audit di implementazione e ordine esecutivo - 2026-09-06

Verifica sul checkout corrente, senza modificare i protocolli in produzione:

- `internal/broker/broker.go:forwardMuxStream` legge soltanto stream ID e
  lunghezza in un header privato di cinque byte. `buildBrokerHeaderLocked`
  genera timestamp e contatori di delivery dopo l'arrivo al broker. Questi
  valori non provano quale frame del worker abbia prodotto due superfici.
- `protocol/packet.go` espone NMB2 versione 1, header 32 byte;
  `internal/broker/analysis_shm.go` e SDK Python espongono NMSH versione 2,
  header 64 byte. L'epoch SHM e' della pubblicazione, non va riutilizzato come
  epoch della cattura. L'aggiornamento comprende anche notifier, SDK e consumer.
- Il worker V4L2 ha la superficie NV12 dopo decode; analysis puo' derivare
  da capture o main a seconda di geometria/cadenza/processing. La retention
  deve agganciarsi alla superficie processata effettiva prima del resize,
  senza dipendere da `emit_main` o attivare l'encoder live.
- Nel decoder RDK l'input compresso usa attualmente PTS zero. Il percorso
  asincrono JPU non deve associare il risultato al buffer USB appena letto:
  deve trasportare un token di submission restituito/attestato dal decoder,
  oppure dichiarare correlazione non disponibile. Stessa regola per VPU e
  qualunque provider capace di ritardare/riordinare output.
- Il worker HBN ottiene i buffer tramite `sp_vio_get_frame` per ruolo; i
  contatori dei ruoli non dimostrano uguaglianza del frame sensore. Va usata
  una cattura comune con fanout oppure un'identita' nativa attestabile:
  chiamate consecutive e timestamp simili non bastano.
- Il percorso GStreamer usa bus `shmsink/shmsrc` e `do-timestamp=true` nei
  processi di ruolo. Un header aggiunto solo al mux finale non recupera la
  provenienza persa lungo il bus. Serve trasportare identita' e trasformazione
  attraverso il confine dei processi, non numerare gli arrivi separati.
- L'adapter tracker conserva frame ID/epoch del delivery in `VideoFrame`,
  ma l'emitter materializza oggi crop dalla sola immagine analysis. Il suo
  `source_same_frame=true` descrive quel prelievo locale, non costituisce una
  proof di correlazione con una superficie source-resolution del broker.
- Il recorder persiste anche pacchetti NMB nei segmenti raw di recovery:
  `internal/recorder/segments.go` scrive tramite `WritePacket`, `mixer.go`
  rilegge tramite `ReadPacket`. Il bump non riguarda solo socket e SDK:
  serve finalizzazione o conversione/export una tantum verificato dei raw
  utili prima del cutover, senza dual reader permanente. Gli MP4 gia'
  finalizzati non dipendono da questo framing.
- `--media-epoch` del worker esiste gia' per il fence fanout: non va duplicato
  con un alias. Per Frame Access il suo dominio deve impedire riuso dopo
  restart del broker, non soltanto numerare i riavvii dentro un processo.
  Un namespace nuovo attestato per processo e contatori monotoni governati
  devono proteggere source e publication da replay; il tempo non associa frame.

Sequenza di lavoro scelta:

1. **Provenienza prima delle immagini piu' grandi.** Fissare envelope unico,
   identita', geometria e test vector; sostituire worker mux e protocolli/SDK
   nello stesso cutover, coprendo V4L2/JPU/VPU, HBN e GStreamer. La feature
   resta indisponibile sui percorsi senza proof; nessun alias legacy o schema
   nuovo parzialmente interpretato.
2. **Accesso bounded indipendente.** Implementare store source-local RAM e
   RPC/FD con ACL distinte dallo stream, eviction non bloccante e invalidazione
   causale. Provider di copy/crop/encode selezionati e attestati, mai dedotti
   dal nome della board. Non introdurre intanto un ring inutilizzato o un
   endpoint che espone immagini analysis ingrandite come processed-full.
3. **Adapter separato.** Tracker richiede ROI immediatamente dopo detection,
   prima di outbox/fsync/embedding; Registry riceve asset durevoli con proof.
   Miss esplicito analysis-only, proof invalida esclusa dall'apprendimento.
   La correzione body-only/face-trust procede in parallelo e non aspetta questa
   estensione; maggiori pixel non sostituiscono il controllo della fiducia.
4. **Qualificazione.** test board multi-source prima, poi deployed board con profilo centrale
   MJPG 1280x720@20 / analysis NV12 640x360@20, hardware obbligatorio. Misurare
   pixel utili, latency, FPS, memoria/FD, CPU/temperatura e scritture SD contro
   baseline; soak canonico >=2700 s e live parked senza encoder aggiuntivo.

Il rapporto 1280x720 -> 640x360 permette geometricamente un ROI due volte piu'
ampio per lato (quattro volte i pixel), prima di padding/alignment/clipping.
Non e' una promessa di miglioramento del riconoscimento: richiede corpus e
misure genuine/impostor senza peggiorare i falsi positivi. Nessuna modifica del
corpus storico e nessun deploy derivano automaticamente da questo audit.

## Motivazione di prodotto

Il broker corrente espone bene flussi continui e stato misurato, ma manca una
primitiva per rispondere autorevolmente alla domanda:

> dato un frame realmente consegnato, restituisci questa regione da una
> superficie correlata dello stesso source frame.

Risolvere il caso identity facendo aprire al tracker sia `analysis` sia `live`
sarebbe una scorciatoia applicativa:

- attiverebbe l'encoder live anche senza spettatori;
- imporrebbe decode H265 nel tracker;
- duplicherebbe banda e copie;
- legherebbe una funzione di frame access alla policy dello stream live;
- non risolverebbe la correlazione quando live e analysis hanno source
  differenti;
- non produrrebbe una prova causale usando gli ID attuali.

Il valore standalone del broker cresce invece se il prodotto possiede anche
identita', correlazione, accesso bounded e trasformazione geometrica dei frame
del proprio graph.

## Coerenza con la filosofia esistente

La proposta estende, senza contraddirli, gli invarianti di `PRODUCT.md`:

- il consumer chiede una source, una superficie e un media contract, non un
  device node o un'API vendor;
- capability dichiarata, provider selezionato e funzionamento attestato
  restano stati distinti;
- processing e conversioni avvengono una volta nello stage appropriato e sono
  riutilizzati;
- consumer lenti non producono backpressure su capture o archive;
- source, failure domain, epoch e stato restano indipendenti;
- software fallback non e' mai silenzioso;
- il broker pubblica media e stato misurato, ma non implementa logica
  applicativa.

La superficie pubblica del prodotto diventa:

```text
source graph
    |
    +--> Stream Plane
    |      live / analysis / archive continui
    |
    +--> Frame Access Plane
    |      Describe / Extract / Correlate on demand
    |
    +--> Status, Capability e Control Plane
           stato, provider, limiti e prove runtime
```

## Nuovi invarianti di prodotto

1. Ogni frame origine riceve un'identita' source-local, causale e immutabile.
2. Ogni frame consegnato conserva una relazione dimostrabile con il proprio
   frame origine oppure dichiara esplicitamente correlazione non disponibile.
3. Timestamp e nearest-frame non sono mai una prova di correlazione.
4. Le superfici sono descritte semanticamente; nomi di board, JPU, GPU o API
   vendor non entrano nel contratto consumer.
5. Frame store, richieste, ROI, byte, FD e tempi sono tutti bounded.
6. L'eviction vince sempre sul consumer: nessuna lease puo' trattenere il
   buffer camera o bloccare il graph.
7. Il broker non persiste frame o ROI su disco.
8. Il risultato estratto appartiene al consumer; retention e workflow durevoli
   sono responsabilita' esterne.
9. Una source non puo' essere sostituita implicitamente con un'altra.
10. Correlazione cross-camera non viene simulata con tempo o prossimita'.
11. Ogni capability pubblicata espone candidate, selected e runtime-attested.
12. Il protocollo e gli SDK non contengono concetti Person Registry, tracker,
    volto, corpo, identita' o Nemesi.

## Terminologia

### `OriginFrameRef`

Identifica l'immagine origine all'interno di un source graph:

```json
{
  "schema": "video-broker.origin-frame-ref.v1",
  "source_ref": "front",
  "source_media_epoch": 8752205360568578,
  "origin_sequence": 5043,
  "graph_revision_sha256": "<64 lower hex>"
}
```

`origin_sequence` e' assegnato nel punto in cui il worker possiede
causalmente il frame decodificato/catturato. Non viene ricostruito dal tempo di
arrivo al broker.

### `DeliveryFrameRef`

Identifica una consegna su un output pubblico:

```json
{
  "schema": "video-broker.delivery-frame-ref.v1",
  "stream_ref": "analysis-front",
  "stream_epoch": 1482897744391221,
  "frame_id": 3912,
  "graph_revision_sha256": "<64 lower hex>"
}
```

Il broker mantiene la relazione bounded:

```text
DeliveryFrameRef -> OriginFrameRef -> frame-store slot + transform chain
```

Un consumer normalmente usa il `DeliveryFrameRef` ricevuto dallo stream. Il
broker risolve internamente l'origine e restituisce un proof, senza obbligare
il consumer a conoscere il mux privato o l'implementazione del graph.

### `SurfaceRef`

Identifica una vista semantica dei pixel:

- `source_decoded`: pixel decodificati prima del processing del broker;
- `processed_full`: pixel full-resolution dopo il processing configurato;
- `delivery:<stream_ref>`: vista esatta che alimenta lo stream indicato prima
  dell'eventuale encoding.

Un provider puo' non rendere disponibile ogni superficie. L'assenza deve
essere dichiarata dalla capability e restituita come errore esplicito.

### `TransformRef`

Identifica la trasformazione esatta fra due superfici: orientation, crop,
letterbox, resize, padding e allineamento chroma. La matematica appartiene al
broker e non viene replicata dai consumer.

### `FrameArtifact`

E' il risultato di una richiesta di estrazione: frame intero o ROI, metadata,
proof, digest e uno o piu' payload trasferiti tramite FD.

## Identita' e correlazione nel data plane

Il mux privato worker -> broker deve essere sostituito con un unico envelope
corrente che accompagni ogni superficie o access unit con:

- `source_media_epoch`;
- `origin_sequence_u64`;
- `capture_monotonic_ns` e timestamp media quando disponibile;
- `surface_kind`;
- geometria e stride;
- `transform_ref`;
- graph revision;
- stato di correlazione (`exact` oppure `unavailable`).

Tutti i worker supportati dalla release producono lo stesso envelope:

- RDK X5 V4L2;
- RDK X5 HBN;
- worker GStreamer portabile;
- futuri provider scoperti dal prodotto.

Se decode, encoder o passthrough non conservano una relazione esatta, il
provider pubblica `correlation=unavailable`. Il broker non usa PTS simili o
frame adiacenti per colmare il vuoto.

I protocolli continui devono rendere ogni frame referenziabile:

- il successore di NMSH trasporta il riferimento di consegna e l'identita'
  origine necessaria al binding;
- il successore di NMB2 fa lo stesso per i frame socket;
- epoch e graph revision impediscono riuso dopo restart, source rebind o cambio
  della geometria;
- gli SDK costruiscono il `DeliveryFrameRef`, evitando composizioni manuali.

Se l'header pubblico cambia, la release introduce una sola versione corrente
di NMSH/NMB e aggiorna tutti i consumer nello stesso ciclo. Non restano reader
o negoziazioni permissive per i protocolli precedenti.

## Frame store source-local

Ogni source graph con Frame Access abilitato possiede un frame store RAM
bounded. Lo store conserva le superfici previste dalla policy di prodotto,
normalmente `processed_full`, indicizzate da `OriginFrameRef`.

Vincoli obbligatori:

- pool preallocato o crescita rigidamente limitata;
- limiti simultanei per frame, eta' e byte;
- nessuna scrittura su SD;
- slot protetti da epoch/sequence e mai letti durante overwrite;
- invalidazione immediata dopo restart, rebind, graph revision o transform;
- nessuna lease sul buffer camera;
- eviction non bloccante;
- un client lento riceve `expired`, `deadline_exceeded` o
  `capacity_rejected`;
- distruzione source-local senza impatto sugli altri graph.

Valori iniziali da qualificare, non default universali assunti:

```text
max_frames = 6
max_age_ms = 750
max_bytes_per_source = 64 MiB
max_requests_in_flight_per_consumer = 1
max_regions_per_request = 4
```

A 1280x720 NV12 sei frame occupano circa 8.3 MiB per source. Il limite byte
rimane sempre autorevole a risoluzioni maggiori. Eta' effettiva e hit window
sono pubblicate nello status, non dedotte dalla configurazione.

Il percorso portabile copia `processed_full` in un pool NV12. Un provider puo'
usare buffer DMA reference-counted soltanto se dimostra che la retention non
trattiene ingress buffer, non riduce la profondita' del codec e non causa
backpressure. La capability selezionata viene attestata a runtime.

## API pubblica

Il Frame Access Plane mantiene una superficie piccola.

### `DescribeFrameAccess`

Restituisce, per source:

- stato enabled/disabled/degraded;
- superfici disponibili;
- stream referenziabili;
- formati output;
- operazioni disponibili;
- limiti effettivi;
- trasporti disponibili;
- provider candidate/selected/attested;
- correlation mode;
- retention window osservata.

### `Extract`

Estrae frame intero o regioni da una superficie correlata.

Richiesta concettuale:

```json
{
  "schema": "video-broker.frame-extract-request.v1",
  "request_id": "<128-bit lower hex>",
  "frame_ref": {
    "schema": "video-broker.delivery-frame-ref.v1",
    "stream_ref": "analysis-front",
    "stream_epoch": 1482897744391221,
    "frame_id": 3912,
    "graph_revision_sha256": "<64 lower hex>"
  },
  "target_surface": {
    "kind": "processed_full",
    "source_ref": "front"
  },
  "regions": [
    {
      "region_id": "roi-1",
      "input_space": "delivery:analysis-front",
      "bbox_px": [120, 44, 188, 132],
      "padding_ratio": 0.20
    }
  ],
  "output": {
    "pixel_format": "nv12"
  },
  "deadline_monotonic_ns": 123456789
}
```

Una regione `full_frame=true` rappresenta l'estrazione dell'intera superficie
ed e' soggetta a limiti byte piu' restrittivi.

Risposta concettuale:

```json
{
  "schema": "video-broker.frame-extract-response.v1",
  "request_id": "<same id>",
  "state": "complete",
  "correlation": {
    "mode": "exact_same_origin",
    "origin_ref_sha256": "<64 lower hex>",
    "graph_revision_sha256": "<64 lower hex>",
    "transform_ref": "<bounded token>"
  },
  "artifacts": [
    {
      "region_id": "roi-1",
      "state": "complete",
      "target_bbox_px": [240, 88, 376, 264],
      "width": 136,
      "height": 176,
      "stride": 136,
      "pixel_format": "nv12",
      "payload_bytes": 35904,
      "sha256": "<64 lower hex>",
      "fd_index": 0
    }
  ]
}
```

Il broker valida coordinate finite, orientation, limiti, padding, subsampling e
chroma alignment. Regioni invalide vengono rifiutate singolarmente senza
leggere fuori superficie.

### `Correlate`

Verifica se due `DeliveryFrameRef` derivano dallo stesso frame origine.
Risultati ammessi:

- `exact_same_origin`;
- `different_origin`;
- `different_source`;
- `expired`;
- `correlation_unavailable`.

Non esiste `probably_same`, nearest timestamp o tolleranza temporale.

## Error model

Stati terminali bounded e osservabili:

- `complete`;
- `expired`;
- `epoch_mismatch`;
- `source_mismatch`;
- `graph_revision_mismatch`;
- `transform_mismatch`;
- `surface_unavailable`;
- `correlation_unavailable`;
- `region_invalid`;
- `format_unsupported`;
- `capacity_rejected`;
- `deadline_exceeded`;
- `unauthorized`;
- `provider_unavailable`.

Un errore del Frame Access Plane non degrada automaticamente gli stream. Lo
status distingue salute del media graph e salute della feature. Un errore che
dimostra corruzione del graph o falso attestation puo' invece degradare la
source secondo la normale policy del broker.

## Trasporto e SDK

Trasporto locale raccomandato:

```text
${runtime_dir}/video-broker-frame-access.sock
```

- Unix `SOCK_SEQPACKET` per preservare i confini dei messaggi;
- metadata versionati e bounded;
- payload tramite `memfd` sigillato e `SCM_RIGHTS`;
- `SO_PEERCRED` e binding alla process generation governata;
- nessun pathname arbitrario;
- deadline obbligatoria;
- chiusura deterministica di tutti gli FD.

Il formato wire puo' essere binario mentre gli esempi di questo documento sono
JSON concettuale. Go e Python SDK devono essere rilasciati insieme al
protocollo e devono occuparsi di:

- costruzione e validazione dei frame ref;
- deadline e cancellazione;
- ricezione e verifica dei seals;
- limiti byte/geometry;
- chiusura FD;
- digest e proof validation.

HTTP, gRPC remoto, WebRTC e API browser non appartengono al core. Un prodotto
esterno puo' costruire un gateway usando l'SDK senza allargare il trust boundary
del broker embedded.

## Capability discovery e provider

Il Frame Access Plane segue la gerarchia gia' usata dal broker:

```text
declared candidate -> selected plan -> runtime attested
```

Capability possibili:

- superfici `source_decoded`, `processed_full`, `delivery`;
- correlazione esatta per ciascun output;
- copy NV12 portabile;
- DMA-backed retention attestata;
- ROI crop/resize hardware;
- JPEG hardware;
- formati output;
- limiti geometry, stride, byte e concurrency.

NV12 e' il baseline portabile. JPEG, RGB, resize hardware o zero-copy sono
provider opzionali espliciti. Un JPU puo' essere usato soltanto dopo discovery,
selection e prova con media reale; il nome RDK X5 non costituisce capability.
Un fallback CPU esiste solo se autorizzato dalla policy e viene pubblicato.

## Configurazione prodotto

La configurazione e' source-local e non nomina consumer applicativi:

```yaml
sources:
  front:
    selector: "..."
    frame_access:
      enabled: true
      retained_surface: processed_full
      max_frames: 6
      max_age_ms: 750
      max_bytes: 67108864
      max_regions_per_request: 4
      output_formats: [nv12]
      correlation: exact_required
```

Policy globali possono definire ACL, quote per consumer e massimi assoluti, ma
ogni source mantiene store, epoch, failure e metriche indipendenti.

L'introduzione del nuovo contratto richiede un bump dello schema prodotto e
una sola versione runtime corrente. Configurazioni utili vengono convertite
una tantum; non rimane un reader del vecchio schema nel servizio.

## Status e telemetria

Ogni source pubblica almeno:

- `frame_access.state`;
- `available_surfaces` e `referenceable_streams`;
- `correlation.requested/selected/attested`;
- `retention.frames`, `bytes`, `oldest_age_ms`;
- `requests`, `hits`, `expired`, `capacity_rejected`;
- mismatch source/epoch/graph/transform;
- richieste unauthorized;
- ROI copy/convert latency p50/p95/max;
- byte restituiti per formato;
- FD e memfd correnti;
- `capture_frames_affected_total`, che deve restare zero;
- provider candidate/selected/attested per copy, crop, resize e encode.

Le metriche permettono di distinguere:

- ring troppo corto;
- consumer lento;
- perdita di correlazione nel provider;
- saturazione memoria o richieste;
- costo CPU/GPU/JPU;
- regressione del capture graph.

## Sicurezza e privacy

Il Frame Access Plane e' piu' sensibile di uno stream gia' autorizzato e deve
applicare:

- autenticazione del peer;
- autorizzazione per source, superficie e operazione;
- quote per identita' consumer;
- limiti per frame intero, ROI, byte e frequenza;
- audit senza includere pixel o segreti;
- cancellazione alla deadline;
- zero persistenza broker;
- invalidazione immediata a ogni cambio epoch/graph;
- memfd sigillati e verificabili.

Un consumer autorizzato a `analysis` non ottiene automaticamente accesso a
`processed_full`. Le policy restano esplicite.

## Integrazione Person Registry come primo consumer

Questa sezione descrive un adapter, non il contratto del broker.

1. Il tracker apre soltanto lo stream `analysis`.
2. NMSH/SDK gli consegna un `DeliveryFrameRef` autorevole.
3. La BPU esegue inference sul frame analysis.
4. Il tracker seleziona bbox head/body.
5. Il tracker chiama `Extract` su `processed_full` usando le coordinate nello
   spazio `delivery:analysis`.
6. Il broker applica il transform esatto e restituisce ROI NV12 e proof.
7. Il tracker crea e rende durevoli crop analysis e crop identity nel proprio
   outbox.
8. Person Registry riceve asset e proof; non interroga il broker.

L'estrazione deve partire prima di fsync, embedding o altri lavori lenti. Il
frame store e' deliberatamente effimero. Registry e outbox possono invece
ritentare dopo minuti perche' lavorano su asset gia' materializzati.

`source_same_frame` viene derivato da `exact_same_origin`, mai accettato come
booleano dichiarativo privo di prova. Un miss produce un'osservazione
analysis-only con motivo esplicito; un proof mismatch e' errore di integrita' e
non aggiorna prototipi.

Il crop ad alta risoluzione proviene dalla superficie `processed_full` della
source legata ad analysis. Non dipende dall'attivazione di live e non richiede
decode H265 nel tracker.

## Source indipendenti e cross-camera

Il frame ref determina sempre la source:

```text
DeliveryFrameRef -> stream.source_ref -> OriginFrameRef.source_ref
```

Esempi:

- se `analysis` e `live` usano `front`, l'estrazione processed appartiene allo
  stesso source graph;
- se `analysis=overview` e `live=front`, una richiesta ancorata ad analysis
  estrae da `overview`;
- associare una persona rilevata da `overview` a un'immagine di `front` e' un
  problema futuro di calibrazione, time sync e subject association.

Il Frame Access Plane puo' dire `different_source`; non esegue fusion e non
produce mai `exact_same_origin` fra camere differenti.

## Non-goal

Il broker non possiede:

- object detection, tracking o scelta delle bbox;
- face/body recognition o embedding;
- Person Registry e prototipi identita';
- persistenza e retention applicativa;
- recorder o MP4 policy;
- cross-camera subject association;
- WebRTC signalling;
- PTZ decisions;
- browser authentication;
- event bus applicativo.

Queste funzioni consumano media e proof tramite SDK e possono evolvere
indipendentemente.

## Zero legacy e dati

L'implementazione deve lasciare un solo contratto corrente per:

- mux privato worker;
- protocolli frame pubblici;
- Frame Access RPC;
- SDK;
- configurazione e status.

Tutti i consumer in repository vengono migrati nello stesso ciclo. Non si
mantengono dual reader, euristiche temporali o fallback al vecchio framing.

Questa regola riguarda contratti e runtime, non autorizza la perdita di dati.
Crop, record e asset storici utili del Person Registry vengono mantenuti se
compatibili oppure convertiti/importati una tantum.

## Piano di implementazione

### Fase 1 - Contratti di prodotto

- aggiungere invarianti a `PRODUCT.md`;
- specificare FrameRef, SurfaceRef, proof, error model e limiti;
- definire bump schema e protocolli correnti;
- generare test vector condivisi Go/Python/native.

### Fase 2 - Identita' nel worker e nei protocolli

- assegnare `OriginFrameRef` source-local;
- sostituire il mux privato per tutti i worker;
- propagare correlazione attraverso decode/processing/encode;
- aggiornare NMSH/NMB e SDK senza reader precedenti;
- attestare i path exact/unavailable.

### Fase 3 - Frame store e RPC

- pool source-local bounded;
- indice Delivery -> Origin -> slot;
- transform chain;
- `Describe`, `Extract` e `Correlate`;
- memfd/SCM_RIGHTS, peer auth, ACL e quote;
- status e metriche.

### Fase 4 - Provider e portabilita'

- baseline copy/crop NV12;
- RDK V4L2, HBN e GStreamer;
- discovery e runtime attestation;
- provider JPEG/resize hardware solo se dimostrati;
- test di equivalenza output fra backend.

### Fase 5 - Adapter Person Registry

- propagare `DeliveryFrameRef` nel tracker;
- eliminare la sequenza emitter come frame identity;
- chiamare `Extract` immediatamente dopo detection;
- rendere durevoli analysis/identity/proof nell'outbox;
- aggiornare ingest e storage Registry con un solo schema corrente.

### Fase 6 - Device qualification

- test di una sorgente test board front1920x1080 a30FPS attraverso il FAP con ROI e
  consumer attivi; failure isolation/fencing conservati, qualifica fisica
  multi-camera differita aperta a B4a;
- test deployed board source MJPG 1280x720@20 e analysis NV12 640x360@20;
- JPU/VPU/BPU ancora attestati e nessun decoder H265 aggiuntivo;
- soak canonico di almeno 2700 secondi;
- misure CPU, RAM, temperatura, SD, FPS e latency p50/p95.

## Matrice di test obbligatoria

### Contratto

- test vector uguali in Go, Python e worker nativi;
- reject schema/version/length/enum sconosciuti;
- epoch e graph revision obbligatori;
- nessun reader dei protocolli sostituiti;
- SDK chiude FD su ogni errore.

### Correlazione

- stesso origin attraverso processed e delivery;
- FPS live/analysis differenti;
- frame drop e queue latest-only;
- reorder codec con PTS preservato;
- correlation lost dichiarata fail-closed;
- timestamp uguale ma origin differente: rifiuto;
- frame ID uguale ma epoch differente: rifiuto;
- source o graph differenti: rifiuto.

### Geometria

- resize, crop, letterbox e orientation supportati;
- bbox a bordo frame e padding;
- coordinate dispari e chroma alignment NV12;
- piu' ROI nello stesso frame;
- confronto pixel-level tra risultato atteso e restituito.

### Risorse e isolamento

- ring in eviction continua;
- consumer fermo dopo ricezione FD;
- request storm e quote;
- frame intero ai limiti;
- memoria stabile e zero leak FD/memfd;
- nessun calo significativo di live/analysis/archive;
- zero capture timeout o recorder drop causati dalla feature;
- Frame Access guasto mentre gli stream restano sani.

### Multi-source

- due source con epoch e frame ID sovrapposti;
- restart di una source senza invalidare l'altra;
- rebind e graph revision;
- ACL differenti per source;
- cross-source sempre `different_source`.

### Qualita' del primo caso d'uso

Confrontare analysis-only e processed-full ROI su corpus reale:

- face detection ed embedding success;
- genuine/impostor distance distributions;
- false accept, false reject e review rate;
- blur, luma e dimensione utile del volto;
- byte e latency ingest.

La maggiore risoluzione deve produrre un miglioramento misurabile senza
peggiorare le soglie di false accept.

## Criteri di accettazione

- il broker espone un Frame Access Plane indipendente dalle applicazioni;
- nessun tipo, path o schema pubblico nomina Person Registry o Nemesi;
- ogni estrazione accettata ha correlazione causale e proof verificabile;
- nessun nearest timestamp o source fallback;
- API e store sono source-local e bounded;
- nessun consumer puo' creare backpressure su capture o archive;
- assenza di client live non riduce l'accesso ai frame processed;
- capability, provider e correlazione sono runtime-attested;
- protocolli e SDK hanno una sola versione corrente;
- test board singola sorgente30FPS e deployed board al profilo centrale20FPS superano test e
  soak; qualifica fisica simultanea multi-camera fuori goal, aperta in B4a;
- il tracker apre solo analysis e rende durevoli gli asset prima dell'evento;
- Person Registry completa retry e restart senza dipendere dal broker;
- dati storici utili sono preservati o convertiti una tantum.

## Impatto stimato

E' una modifica trasversale medio-grande, ma aumenta una superficie di prodotto
riusabile invece di introdurre un'integrazione ad hoc. Coinvolge:

- definizione prodotto, schema e protocolli;
- worker nativi e portabile;
- broker Go, graph, frame store, sicurezza e status;
- SDK Go/Python;
- capability discovery e provider;
- tracker/outbox e Person Registry come adapter iniziali;
- setup contract e qualification device.

Una volta implementata, l'aggiunta di un nuovo consumer di frame puntuali non
richiede nuovi stream, decoder duplicati o modifiche al data plane del broker:
richiede soltanto un adapter autorizzato che usi il contratto Frame Access.
