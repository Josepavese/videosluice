# deployed board: FAP rifiuta il profilo con tone mapping attivo

Stato: **aperto, bloccante per il sotto-rilascio test board -> deployed board**.
Ambito: video-broker/FAP e ciclo di rilascio delle risorse native OpenCL.
Non e' un nuovo gate di continuita' del Person Registry.

## Evidenza verificata

Il candidato firmato SHA256
`fc908a7242d10b9f45e2b3294758921b1814ee124ad6fe815f79f5071f25396f`,
sequence1789244163760113403, ha superato il soak test board2700s sul suo profilo.
Durante la prova di avvio deployed board del12settembre23:19-23:27UTC, lo stesso
candidato seleziona correttamente MJPG1280x720@20, decode/encode `native_sp`,
ma dichiara `tone_mapping=true` e rifiuta il frame source prima del capture:

```
native frame source profile is not admitted: frame bootstrap provider admission unavailable
```

Il Tracker fallisce poi l'admission per `routing-plan.json` assente: e' una
conseguenza del mancato avvio della sorgente, non prova di un problema JPU,
temperatura, SD o autenticazione. Config, runtime.env e override deployed board hanno
gli stessi hash del preflight. Nessun parametro o soglia e' stato modificato.

`cmd/video-broker/frame_product_linux.go:composeRDKFrameSource` esclude
esplicitamente `ToneMappingEnabled`; il test `tone-unconfirmed-release`
conferma che l'esclusione e' intenzionale. In
`native/rdkx5_v4l2_mux_bridge.c` il receipt di teardown non puo' confermare il
percorso tone: `nv12_tone_mapper_accelerator.h` scarta gli esiti `clRelease*`.
Il successo test board senza tone non copreva questa differenza del profilo deployed board.

## Correttiva necessaria prima di una nuova promozione

1. Lasciare terminare la transazione corrente e verificarne il recovery
   canonico; non forzare health, inhibitor, trial o attivazione.
2. Conservare il profilo deployed board. Non risolvere togliendo il guard oppure
   disattivando silenziosamente il tone mapping o il FAP.
3. Introdurre esiti verificati del rilascio OpenCL e propagazione degli
   errori nei percorsi start parziale, cambio geometria, governor, fanout
   e stop. Un rilascio incerto non deve diventare una ricevuta positiva;
   conservare le lifetime dei buffer finche' la risorsa non e' scaricata.
4. Logging dell'admission con la causa specifica, non solo provider generico;
   regressioni di admission e fault injection sul rilascio nativo.
5. Qualificare su test board il percorso tone/FAP pertinente prima di confezionare
   un nuovo artefatto; riusare le prove che restano valide. Poi soak ufficiale
   del candidato corretto e solo dopo promozione deployed board e suo soak.

Il PASS test board del candidato originale resta una prova valida per quel
profilo, ma **non autorizza a forzare il candidato su deployed board**. Nessun soak
deployed board e' stato avviato o dichiarato positivo.

Evidenze private:
`<private-workspace>`
(`deployed-board-trial-admission.log`, `deployed-board-trial-readback-2.log`, `deploy-deployed-board.log`).
Esito terminale23:39UTC: nuova release respinta, updater e controller exit1;
precedente a94ff6 ripristinata dal recovery canonico con configurazioni
identiche. Postflight della release precedente PASS15s e accesso runtime
valido; supervisor238360, invocationaae66480ceae4ac29817e58490def91c,
NRestarts0. Ricevuta `deployed-board-rollback-readback-4.log`. Non ripetere questo
deploy e non chiamare il rollback riuscito un PASS del candidato.

## Correttiva implementata, qualifica integrata ancora aperta — 13 settembre

Rilascio OpenCL verificato per ciascun oggetto e drenaggio delle operazioni
pendenti prima di liberare host storage. Un errore conserva gli oggetti incerti,
impedisce fallback/reinizializzazione e propaga uscita65 al contratto worker;
non produce ricevute positive. Il normale blocking read confermato evita un
ulteriore `clFinish` per frame. Propagazione su V4L2, HBN e GStreamer; per
quest'ultimo gli oggetti USE_HOST_PTR usano storage persistente, non mapping
GstFrame che possono scadere. Questo non qualifica altri SDK/provider HBN.

Semantica verificata nella documentazione ufficiale di
[clReleaseMemObject](https://registry.khronos.org/OpenCL/specs/unified/refpages/man/html/clReleaseMemObject.html)
e [clFinish](https://registry.khronos.org/OpenCL/specs/unified/refpages/man/html/clFinish.html).
L'admission FAP mantiene tutti i vincoli codec/trasporto/HW: ammette il tone
solo dopo questa correzione del ciclo nativo, e ora riporta i campi della causa
invece del solo errore generico. Frame originale pubblicato prima del processing.

Test locali PASS: fault injection delle16 operazioni di release, code pendenti,
partial init, cambio superficie, divieto di fallback con rilascio incerto,
retention fanout/cutover V4L2, ASan/UBSan, contratto HBN, GStreamer CPU,
admission del prodotto e race broker/source. Su test board fixture OpenCL **reale**
PASS00:04:50UTC: parita' pixel global/local640x360 e local1920x1080, destroy
confermato. Libreria vendor installata `/usr/hobot/lib/libOpenCL.so`;
nessun pacchetto modificato, supervisor invariato e massimo82,274C al readback.

Workspace privato `opencl-release.K1TwfG90` sotto il medesimo nemesi-work;
log `test-board-proof-vendor-result.log` e `native-contract-final.log`. PAL fixture
`/var/lib/nemesi/state/operator-backups/opencl-release-1p9H0PJX/proof-vendor.log`.
Mancano ancora integrazione tone/camera/FAP su test board e i nuovi deploy/soak:
questo risultato isolato non chiude l'issue e non autorizza da solo deployed board.

Aggiornamento00:45UTC: build ARM broker/native PASS, massimo84,961C;
GStreamer OpenCL reale60frame1280x720 PASS. Revisione del rilascio condiviso:
prima di cutover/rollback/retire vengono ora scaricati anche i riferimenti
USE_HOST_PTR alle superfici dei ruoli, non solo gli acceleratori candidati.
Test fault/ASan/UBSan di tutti e tre i confini PASS, senza lavoro aggiunto al
percorso per-frame. La qualifica deve usare anche questo delta finale.

Il nuovo probe hardware opt-in `frame_tone_hardware_test.go` usa camera reale,
decoder HW, OpenCL e FAP, con ROI e riavvio. Tre esecuzioni preliminari non
hanno promosso nulla: prima EOF del canale stdin nel banco di test; poi
analysis19,997FPS/rilascio nativo confermato ma richieste ROI troppo anticipate;
infine rifiuto esplicito `capacity_rejected` durante staging concorrente.
Il probe ora conserva lo stesso riferimento e tratta solo `expired` e
`capacity_rejected` come retry limitati allo stesso deadline100ms; non cerca
un frame alternativo. Un errore persistente resta un FAIL, non un crop valido.
Include archiveH265640x360@15 AVBR750 verso sink non persistente per esercitare
anche tone sulla sorgente/VPU. Front test board ha MJPG1280x720@30/25/15, non20:
la qualifica usa30, mentre il profilo centrale deployed board resta invariato a20.
Nuova prova e promozione misurata in corso nel PAL privato
`/var/lib/nemesi/state/operator-backups/opencl-final-cWiMgT5w` (`chain.log`).
Stato ancora **aperto**: nessun nuovo artefatto firmato o soak deployed board positivo.

## Diagnosi della cadenza MJPG e prova della correttiva

13settembre00:59 e01:12UTC: H2651920x1080@30 passa il medesimo test hardware,
analisi19,995FPS e10ROI native con prova di dominio/cattura e geometria reale.
MJPG1280x720@30 completa10ROI e conferma teardown OpenCL/JPU/VPU, ma analisi
10,848 e10,667FPS: non e' un PASS di prestazioni. Nessuna promozione eseguita;
test board torna sempre alla stessa release firmata con misura e configurazioni
invariate. Nessuna modifica all'autoesposizione o al profilo deployed board.

Aggiunto il riepilogo dei contatori gia' esistenti anche all'arresto: le prove
brevi non raggiungevano il primo report periodico. Nella finestra20,1s:
cattura603frame/30,01FPS, zero timeout/corruzioni, sub215, archive213,
decode_no_output188 e nessuna sostituzione nella coda d'ingresso JPU.
Questa perdita non proviene dalla cadenza USB. Poll JPU eseguito solo prima
dell'attesa capture: una decodifica completata durante l'attesa resta esclusa
dal tick, con ulteriori perdite dovute alla cadenza indipendente dell'analisi.

Correttiva in verifica: un secondo poll NON bloccante dopo restituzione del
buffer UVC, soltanto se il primo non ha selezionato un risultato. Nessuna
sostituzione di origine, nessuna attesa attiva, fallback o soglia cambiata.
Contatore `mjpeg_completion_cadence.post_wait_frames` rende osservabile il
percorso recuperato; test hardware identico riusato senza abbassare i criteri.
PAL diagnostico `operator-backups/mjpeg-cadence-cEz6EDCo`; nuovo probe
`operator-backups/mjpeg-postwait-TbpzvXP8`. Esito correttiva ancora pendente.

Esito terminale01:18:31UTC: **prova correttiva PASS**, stessa camera, profili,
criteri e binario di test, nessun cambio AE. H265 analysis20,040FPS;
MJPG19,985FPS,10ROI native per formato, checked teardown confermato.
MJPG:603capture/20,1s=30FPS,402analysis,301archive, decode_no_output0,
queue replacements0, timeout/corruzioni0;101completamenti recuperati dal
secondo poll non bloccante. Questo confronto misura il miglioramento dal
precedente10,667FPS, non lo deduce da una microbenchmark. Native build74,843s,
Tmax83,154C; probe50,924s/Tmax76,949C. Contratto nativo completo con ASan/UBSan
PASS (`native-contract-postwait.log`). Sorgente V4L2 SHA256
729a0c1f6803977e04a589003cc1c0f4bbfcd0c431882bc8622abd820ca153b9.

test board torna alla stessa release firmata misurata; nessuna promozione di sviluppo.
Il delta qualificato viene consolidato direttamente nel prossimo artefatto
ufficiale per evitare una clonazione completa intermedia sulla SD. Restano
obbligatori postflight/soak del candidato firmato test board e poi deployed board; l'issue
non e' ancora chiusa. Nero/scena senza persone non prova qualita' visiva,
Registry sotto carico o completamento del goal generale.
