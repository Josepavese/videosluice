# Front: cadenza HEVC ridotta, recuperata dopo confronto della cattura

Stato: **APERTO — causa interna non attribuita; recupero runtime osservato**.
N6 del goal FAP: non declassare a backlog un calo che impedisce i20FPS utili.
Hardware osservato test board front2bc5:0529, USB1-1.4; nessuna policy universale
derivata da questa sola camera.

## Evidenze e distinzione dalla CPU Tracker

Con root6a82b721 e source HEVC1280x720@25, native e Tracker ricevevano circa
10.8FPS. DQBUF breve tracciato mostra timestamp del buffer distanti circa92ms.
La traccia ptrace e' diagnostica/perturbativa, non misura di qualifica. Il
successivo confronto senza broker/decode/Tracker conferma il comportamento.

11settembre00:44UTC, runtime fermato con guard e ownership verificati, stessa
camera/scena/autoesposizione3 e controlli invariati:

- HEVC1280x720@25:90buffer, sequence0..89 senza buchi;89intervalli in8.195221s
  =10.85999FPS. Ultimi30buffer10.78974FPS; mediana91.998ms. Tempo totale
  apertura+cattura8.397s. Non dipende da inferenza o hash dei frame.
- MJPG1280x720@30:90buffer, sequence0..90 con UN buco nella fase iniziale;
  pausa iniziale968ms seguita da burst. Media26.49038FPS, ultimi30buffer
  29.96202FPS; mediana32.012ms. Tempo totale3.529s. Non30FPS end-to-end:
  nessun decode JPU, modello, Registry o ammissione di questo modo.
- Dopo il ritorno automatico alla STESSA configurazione HEVC@25, il broker
  pubblica1501frame/60s=25.01FPS, analysis1200=20 e archive900=15, zero timeout/
  corruzioni, resize1200 nel percorso NEON (CPU/SIMD, non scaler dedicato).
  Quindi il basso rate non e' un limite costante
  del formato HEVC e il solo confronto A/B non ne prova la causa interna.

La sequenza di riconfigurazione ha preceduto il recupero. Da distinguere ancora
stato interno UVC/camera, negoziazione e comportamento dell'autoesposizione.
Il valore exposure100 marcato inactive NON e' una lettura dell'esposizione
effettiva. Non disabilitare AE o introdurre toggle automatici di codec come
workaround generico senza una qualifica e una spiegazione riproducibile.

## Preservazione e lavoro residuo

Unit `nemesi-front-capture-ab-20260911T0045Z.service` terminata0; nessun deploy.
Stessa root6a82b721, misura9303entry/553082580byte PASS. Config, override,
runtime.env e controlli camera preservati. Runtime riavviato00:45:05UTC,
supervisor1918922, broker1918970, native1919182. Routing single-front rev98
invariato; MJPG non abilitato nel catalogo. Nessun dato personale modificato.

Verificare tenuta degli FPS utili/risorse e tenere separata la qualifica dopo
restart dalla prova di cold boot. Non dichiarare corretto il difetto solo
perche' ora la cadenza e' tornata normale. Nessun nuovo rilascio per soli log/docs.
Primi121.881s misurati dopo il recupero: Tracker16.894/delivery19.946, zero
read_errors/stessoPID1919242/Tmax81.346C/BPUmedia35.28. La cadenza camera25
tiene nei riepiloghi fino00:50UTC, ma resta un limite separato nel consumer.

Prove persistenti `fap-20260909-bvKbZ4qT/crop-orientation/capture-ab-*`,
`front-capture-*`, `front-restored-rate-resource.jsonl`. Controller diagnostico
SHA9410a2ae2e91ac3597ba31a7188bd4d461c820c1bdac43f713d48c932758aa4c,
PAL `operator-backups/tracker-selected-pose-Ho0qwErQ/front-capture-ab.py`.
Controller single-use: non riavviarlo con i vecchi PID/claim.

## Confronto esposizione A/B/A, 11 settembre23:46-23:48UTC

Stessa front2bc5:0529, HEVC1920x1080@30, stessa acquisizione UVC aperta per
tutte le fasi, senza decoder/FAP. Tre gruppi di180buffer; primi60 esclusi
dalla misura di regime per ogni transizione,120utili, nessun salto sequence:

- Automatica iniziale (`auto_exposure=3`): **12.95849FPS**.
- Manuale temporanea (`auto_exposure=1`, `exposure_time_absolute=100`):
  **29.99270FPS**.
- Automatica ripristinata (`auto_exposure=3`): **29.99282FPS**.

Il controllo assoluto usa unita' di100microsecondi secondo la
[documentazione V4L2](https://docs.kernel.org/7.0/userspace-api/media/v4l/ext-ctrls-camera.html).
Il valore100 inattivo in modalita' automatica non misura l'esposizione reale.
La transizione dei controlli ha recuperato la cadenza anche tornando in
automatica: evidenza di uno stato della camera influenzato dall'esposizione,
**non prova di insufficiente luce, guasto sensore o un generico limite AE**.
Nessun toggle automatico aggiunto al prodotto, nessuna policy universale.

Autoesposizione ripristinata e verificata; nessun cambio di formato, FPS,
configurazione persistente, soglia, firmware o release. Controller
`nemesi-front-exposure-ab-20260911T2350Z` terminato0; misura esatta
9329entry/553388337byte PASS. Root2dd3c1a1 invariata, runtime ripartito23:47:50UTC.
Backup/prove PAL `operator-backups/front-exposure-ab-LbmkGb2Q/`:
`probe.py`, `capture.log`, `summary.json`, `checkpoint.json`, `observer.log`.
SHA controller77deb4dee9fa4f509f9f6014bd6328d829ec364c8162b7e031267df768cd3bc6.

Osservazione integrata successiva186.978s, stesso PID/generazione Tracker:
source30FPS, analysis20, archive15 nei riepiloghi nativi; zero corruzioni,
timeout e scarti source; Tracker **17.515FPS**, read_errors0/reopens0,
Tmax81.444C. Il recupero30 persiste oltre la breve fase raw e il riavvio del
servizio; non e' ancora una prova di tenuta a lungo termine/cold boot, di
qualita' visuale o di due sorgenti reali. Il limite residuo del Tracker e'
distinto dal precedente13FPS della camera. Issue ancora APERTA.
