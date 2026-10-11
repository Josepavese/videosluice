# test board overview: circa25fps effettivi con richiesta30fps

Stato: osservato; origine precisa della cadenza camera non ancora isolata.
Non modificare centralmente limiti o autoesposizione sulla base di questo
singolo campione HW. Separato dal difetto delle generazioni live gia' corretto.

## Evidenze

test board, release `1ecd1414...`, capture overviewPID1732217/epoch1:

- Nel primo intervallo nativo delle00:10, prima dello switch e con live
  spento,1501frame/60s =25,01fps; successivamente1502/60s =25,02fps.
  Zero capture timeout, frame corrotti o decode senza output; lavoro medio
  per frame5,33-6,18ms e attesa media33,76-34,64ms.
- Analysis resta circa20fps, archive15fps. Sotto due consumer l'encoder
  live e' configurato fisicamente H2651280x720/30fps/4096kbps, ma i contatori
  VPU e i pacchetti ricevuti confermano circa25,026fps. Nessuna coda crescente
  osservata nelle prove90/90/75s: gap massimo60,1ms, timestamp sul socket
  circa2ms p95. Questi timestamp non misurano la latenza ottica end-to-end.
- Il FD del capture identifica `/dev/video0`. Lettura senza mutazioni:
  `VIDIOC_G_PARM` dichiara30fps; auto_exposure3 Aperture Priority,
  power_line_frequency1 (50Hz), bilanciamento del bianco e autofocus automatici.
  Il valore4096 di exposure_time_absolute e' marcato inactive: non prova il
  tempo di esposizione effettivo in automatico. La lettura del controllo
  opzionale Exposure Dynamic Framerate restituisce EIO5: stato non acquisito.

La successiva prova prolungata conferma il dato: due consumer simultanei
completano2850s con71324frame ciascuno,25,026fps, gap massimo62,9ms e
timestamp age p95 di1,884/1,902ms. Il soak canonico2700s interamente coperto
da questi consumer passa, massimo83,447C. La durata piu' lunga rafforza la
misura della cadenza, non identifica la causa camera ne' certifica30fps.

La cadenza25fps e' quindi gia' presente all'acquisizione, anche senza encoder
live; non e' un limite di erogazione introdotto dal player WebRTC. Il ruolo
eventuale di luce, autoesposizione, anti-flicker o firmware camera resta una
ipotesi, non una causa certificata. Nessun controllo camera e' stato cambiato,
nessuna soglia di test ridotta e nessun nuovo profilo25fps applicato.

## Prossime verifiche

Confrontare condizioni di illuminazione e timestamp/cadenza UVC con identita'
e modalita' della stessa camera verificate. Qualunque prova che modifichi un
controllo deve restare locale a questa camera, esplicita, reversibile e con
ripristino verificato. Mantenere autoesposizione come feature; non introdurre
una policy generica per tutte le camere. I30fps rimangono non qualificati in
queste condizioni, indipendentemente dal successo di park/resume e dal soak.

## Temporizzazione WebRTC verificata

Il codice di ingest gia' distribuito passa a Pion la differenza tra timestamp
di access unit successivi, non un intervallo fisso derivato dai30fps nominali.
Il nuovo test `webrtc/internal/media/camera_timing_test.go` attraversa il vero
ingest broker e la coda peer con intervalli40/20/80/39,958ms, mantenendo il
profilo nominale30fps. Dieci run con race detector verdi; una mutazione SOLO
in overlay temporaneo che forza33,333ms fallisce sul primo intervallo40ms.
Suite WebRTC completa con race detector e `go vet` verdi. Test e overlay non
sono stati eseguiti sul device; la copia mutata e' stata rimossa.

Il file di produzione `webrtc/internal/media/camera.go` e' rimasto invariato
e confrontato byte per byte con lo ZIP `1ecd1414...`. Si aggiunge soltanto
copertura di regressione: nessuna nuova correzione runtime o nuovo deploy e'
necessario per questo controllo. Questo non sostituisce una prova browser
end-to-end e non chiude l'indagine sulla cadenza della camera.

Evidenze fuori repo in `/tmp/nemesi-test-board-live-control-release.NYx1nVkk/`:
`1ecd-epoch-cadence-events.log`, `1ecd-overview-camera-readonly.txt`,
`1ecd-running{1,3}-comparison.json`, `1ecd-pair{1,2,3}-{a,b}.jsonl`,
`1ecd-loaded-soak-comparison.json`, `1ecd-long-media-{a,b}.jsonl`.
