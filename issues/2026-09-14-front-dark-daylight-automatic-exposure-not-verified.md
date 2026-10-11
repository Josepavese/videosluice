# Front scura anche di giorno: automatico dichiarato, esposizione non verificata

Stato: **APERTO — recupero della luminosita' dopo spegnimento/riaccensione
riferito dall'utente15settembre; causa e correttiva permanente non qualificate**.
Segnalazione diretta dell'utente14settembre: test board resta molto scura anche di
giorno. Requisito: funzionamento automatico, senza tarature manuali a ogni
avvio. La sola ipotesi della stanza poco illuminata non e' sufficiente.
Ulteriore conferma dell'utente: con la stessa orientazione i dettagli interni
erano visibili l'11settembre. Trattare come regressione rispetto a quella data,
non come un limite normale della stanza o una semplice nuova inquadratura.
Assegnazione: N1/N6, qualita' reale dell'ingresso video; attivita' puntuale
richiesta dopo lo STOP del sotto-rilascio. Non riattiva le altre attivita'.

## Aggiornamento15settembre: recupero dopo power cycle riferito dall'utente

L'utente comunica: «Spegnendo e riaccendendo il dispositivo la camera di test board
e' tornata luminosa». Registrato come osservazione diretta dell'operatore,
non come misura strumentata o verifica remota effettuata dall'agente. Non sono
stati acquisiti nuovi frame, controlli UVC, boot ID o log in questo turno.

Questo rafforza l'ipotesi di uno stato anomalo azzerato togliendo alimentazione,
incluso il possibile stato AE/ISP/firmware camera; non dimostra quale componente
fosse responsabile. Sono stati riavviati insieme test board e camera, non soltanto
la camera. Non noti istante/durata del distacco, identita' boot post-recovery,
condizioni di luce prima/dopo o configurazione effettiva dopo il ritorno.
Le precedenti prove di chiusura/riapertura della sorgente non avevano migliorato
front; l'esito del distinto reboot software del14 resta da leggere nel suo
checkpoint, non viene dedotto retroattivamente dal power cycle manuale.

Recupero operativo riferito, NON fix permanente, verifica dinamica AE o nuova
qualifica di stabilita'. Alla ripresa autorizzata preservare lo stato ora buono
e confrontarlo con le prove conservate prima di altri reset/toggle. La notizia
non riattiva autonomamente interventi, monitor, deploy o indagine deployed board;
nessun profilo centrale o dato modificato. Issue ancora aperta.

## Cosa e' verificato

- Root test board qualificata `054b59c0-c3d8b4b86353`, supervisor1563375,
  invocation `ba761ced382f4af4abd621b08ab1b5e6`, NRestarts0;
  config, override, runtime.env e preset invariati rispetto al rilascio.
- Catalogo corrente: front `/dev/nemesi-video-sources/front`,
  H2651920x1080@30, `control_profile=auto`, nessun controllo esplicito,
  optimizer/tone mapping OFF e osservatore qualita' disabilitato.
- Sorgente front in marcia a30FPS, analysis20/archive15; il live usa
  `native_passthrough_static`, quindi non e' ricodificato o oscurato dal tone
  mapper del broker. Nessuna nuova scrittura ai controlli camera durante questo
  controllo. Non confondere front con overview, anch'essa presente nel catalogo.
- `internal/broker/camera_controls.go` locale e installato identici, SHA256
  `4e3c53a0f17cc343885cc8345f41c81ec13fe054827a7a303609bdd01c0ba4bf`.
  Il ramo `auto` elimina i controlli espliciti e non esegue GET_CUR/SET_CUR.
  Log runtime conferma `ownership=camera action=no_uvc_control_io`.
- Pertanto **auto significa delega al firmware, non garanzia di AE attivata,
  misurata o recuperata**. Il commento motivante segnala il rischio di blocco
  endpoint/STREAMON su moduli UVC. Questo rischio non prova che ogni camera
  sia affetta e non giustifica una futura policy indiscriminata.

## Distinzione dalle prove precedenti

### Fotogramma diurno verificato oggi

Estratto un singolo frame a offset550s dal segmento MP4 gia' finalizzato
`/var/lib/nemesi/recordings/mixed/20260914-092000.mp4`, senza seconda cattura
e senza modificare il runtime. Decodifica diagnostica offline a un thread:
non e' un fallback software del prodotto. Immagine640x360 vista direttamente:
finestra luminosa in alto a sinistra, soffitto e stanza quasi neri.

FFmpeg signalstats YAVG18,4157/255, YMIN0/YMAX255. Analisi dell'immagine RGB:
82,68% dei pixel sotto20/255,3,71% sopra230; luminanza media delle celle4x3
da0,14 a123,47. Sono misure del frame archiviato, non lux o letture del sensore.
La distribuzione rende il controluce/misurazione AE della scena un'ipotesi
concreta, non dimostra AE disattivata o guasto hardware. Non e' soltanto CSS
del player. Non identifica da sola quanto dipenda da camera e quanto dalla
decodifica/ricodifica dell'archivio; il live nativo resta il confronto da fare.

Prove private nel PAL workstation `consolidated-hotfix.J41MC68G/`:
`test-board-daylight-archive-20260914T092910Z.png`, SHA256
`e0cb90f3cdfa3de1064d1426d8fd1ffe52a3ab697208d23c2708bb1e12a0c718`;
log omonimo SHA256
`3237a88ac5111956b47c583471e09e6412677e980a32fefff31c271f374a3481`.
Nessuna immagine privata in Git.

### Controlli hardware storici

Confronto aggiunto dopo la precisazione dell'utente: un frame a offset550s
dai segmenti finalizzati `20260911-092000.mp4` e `20260912-092000.mp4`,
stessa procedura diagnostica del14. L'11 sono chiaramente visibili interno,
arredi e persona (nessuna identificazione effettuata); il12 il soffitto e'
quasi nero. YAVG rispettivamente106,265 e9,82033, SATAVG11,827 e0,250833.
Inquadrature e illuminazione non sono identiche, quindi non e' un A/B causale
ne' un calo percentuale misurato sulla stessa superficie; documenta comunque
immagini utili l'11 e molto scure gia' il12, coerentemente con la segnalazione.

I log conservati mostrano frontH2651920x1080@30 in cattura dalle20:46 locali
dell'11settembre. L'aggiornamento centrale era avvenuto alle00:07 locali,
ma la sua ricevuta dice esplicitamente che non aveva applicato il runtime;
non confondere i due eventi. Il profilo centrale precedente aveva gia'
optimizer/tone/quality OFF e controlli camera delegati: non sono disattivazioni
introdotte dall'ultimo sotto-rilascio del13settembre.

La prova AE A/B/A e' datata11settembre23:46–23:48UTC, cioe' **12settembre
01:46–01:48 Europe/Rome**. Il checkpoint originale riletto oggi conferma
`control_changed=true`, `restored=true`, AE3/exposure100 originali.
Ordine temporale da indagare: ultimo giorno indicato buono -> cambio modo
effettivo e test AE -> primo giorno documentato scuro. Non basta a scegliere
fra stato AE/ISP dopo il toggle, comportamento del modo1080p30 e altre
variazioni del percorso. Prima di una correttiva serve isolare queste ipotesi,
senza ripetere il toggle auto/manuale come presunta cura universale.

Prove private accanto al frame del14:
`test-board-daylight-archive-20260911T092910Z.png` SHA256
`8d9a652673816de4d29f68890e533a2ca289327e4a7dfe2acfc2a164a03985c4`;
`test-board-daylight-archive-20260912T092910Z.png` SHA256
`ee4e3f9ab4c9e1f5d83d21638d361715c2fbbe4b34f5f439bdfc4f26a7a4b286`.
Log signalstats omonimi; immagini private fuori Git.

L'11settembre una prova A/B/A porto' temporaneamente la front in manuale,
poi ripristino' e verifico' AE3; aumento' la cadenza, non qualifico' la
luminosita'. Il12settembre controlli AE3/AWB1, gain15 (default5),
brightness/contrast/saturation128. Il valore exposure100 era `inactive`:
non misura l'esposizione reale. Queste sono letture storiche, NON odierne.
Vedi [issue cadenza](2026-09-11-front-camera-rate-state-after-format-switch.md).

La [documentazione V4L2](https://docs.kernel.org/userspace-api/media/v4l/ext-ctrls-camera.html)
distingue AE, tempo assoluto, compensazione e priorita' della cadenza;
scritture manuali del tempo con AE attiva hanno effetto indefinito. Non
prescrivere numeri di controllo o toggle auto/manuale universali.

## Correttiva da qualificare, non ancora implementata

### Prova diretta del14settembre10:03UTC

Su richiesta di ripristino dell'utente, test UVC a runtime quiescente con
update lock, misura esatta, drain, stop, verifica unico proprietario e recovery.
Nessuna scrittura al tree misurato o alla configurazione. Camera front fisica
2bc5:0529 su USB1-1.4, `/dev/video2`, HEVC1920x1080@30.

- Sorgente HEVC diretta prima del cambiamento:120frame,29,989FPS stabili,
  zero buchi di sequenza, immagine1920x1080 scura, YAVG23,0272.
- La camera dichiara menu AE0=Auto Mode, default0, AE3=Aperture Priority.
  Scrittura del solo AE0 accettata e riletta; nessun tempo/gain manuale.
- Dopo AE0:240frame,30,014FPS stabili, zero buchi, YAVG23,0759.
  **Nessun miglioramento ottico misurabile**, confermato vedendo entrambi i PNG.
  Questo esclude una causa esclusivamente a valle della camera (broker/player),
  ma non dimostra ISP spento o identifica il motivo interno della sottoesposizione.
- AE0 mantenuta temporaneamente, non salvata centralmente e non dichiarata
  correttiva. Runtime ripartito10:04:19UTC, supervisor1777061,
  invocation52677f20d2da4452b2491840b1af4158; stessa root/config.
  Misura esatta prima/dopo PASS:9358entries/553727013bytes.
- Prima esecuzione abortita prima di qualsiasi SET per parsing della label
  V4L2; parser corretto e7casi PASS. Non e' un secondo tentativo ottico.

Prove private: workstation `test-board-ae-20260914.N5Rkwvew`, device
`/var/lib/nemesi/state/operator-backups/front-auto-restore-r2.3eiT7SAs`.
Controller SHA256 `6f445c10ed8d7ba2ccc4b7fc35c4475ccf4a3e61c591c46a9608e2836277fc90`;
PNG baseline `b0600194ce632e1d515e8fe27a8594bf29f6d02b3936740a3b9fc974cbfed11b`,
full-auto `6cd8ff6888569c0e04390d1df35c2e5eb18a0c7e62968b85d44cf3daa0ae3865`.
Unita' single-use conclusa, non rieseguire.

### Confronto formati e uscita integrata11:28-11:38UTC

Capabilities effettive confermate: MJPG1920x1080@30. Primo controller di
confronto interrotto PRIMA del cambio formato:240buffer HEVC,8sequence mancanti
fra164 e173,28,886FPS di regime; recovery corretto. Non prova di stabilita'.
Il frame90 precedente al buco e' stato estratto successivamente e visto:
**interno gia' leggibile prima del passaggio a MJPG**, YAVG101,943.
La lampada diventa visibile e il sole entra direttamente. Successivamente
l'utente ha confermato di NON avere acceso luci o toccato/spostato la camera:
non attribuire la differenza a un suo intervento. La diversa resa della lampada
non dimostra che prima fosse spenta; rimane possibile una variazione naturale
della luce. Non attribuire questo recupero al successivo cambio di formato.

Confronto completato11:32-11:33UTC, AE0 invariata, nessun tempo/gain manuale:

| Fase | FPS osservati | Sequence mancanti | Esito ottico |
| --- | --- | --- | --- |
| HEVC prima |29,176|6|Frame180 grigio/corrotto, YAVG128,107 non interpretabile come esposizione|
| MJPG |29,997|0|Frame leggibile, YAVG97,3648|
| HEVC ritorno |18,125|137|Frame selezionato leggibile, YAVG95,189; cadenza non valida|

I salti vengono registrati separatamente nella prova ottica, NON accettati da
una qualifica di cadenza. Non cambiato alcun gate di prodotto/soak. La prima
lettura della media128 e' stata corretta dopo ispezione visiva: era un
artefatto grigio, non miglioramento. Le catture raw scrivevano su SD; separare
eventuale perturbazione del probe da difetti presenti nel flusso di prodotto.
I salti non dimostrano da soli guasto ISP/encoder o SD difettosa.

Runtime ripreso11:33:51UTC, root e quattro fingerprint configurazione
invariati, misura esatta PASS9358entries/553727013bytes. Supervisor1797790,
invocation6a32a429789b4e639e98077ca2aee61c, broker1797832, native1797999.
Riepiloghi nativi successivi60s:30,02 e30,00FPS, analysis20/archive15,
zero capture timeout/corruzioni, tone0, resize nel percorso accelerato.
Un readback diagnostico autenticato ha restituito uno stato valido iniziale
ma la lettura successiva del supervisor e' andata in timeout: NON PASS30s
ne' nuova qualifica di stabilita'. Non allungate le deadline per nasconderlo.

Verifica integrata successiva senza stop o secondo lettore camera: consumer
del socket pubblico live NMB2, peer broker autenticato PID1797832/UID999,
snapshot supervisor autenticato, root/invocation/inode stabili. Ricevuti
270pacchetti H2651920x1080, reset iniziale e ID7182..7451 contigui, circa9s
di timestamp sorgente. PNG estratto leggibile, YAVG97,2931. Anche dopo restart
il video di Nemesi e' dunque visibile; **non prova ancora automatismo
robusto o causa della regressione mattutina**. Tutti i consumer diagnostici
sono chiusi e il formato centrale H2651920x1080@30 e' ripristinato.

Prove private secondo confronto:
`/var/lib/nemesi/state/operator-backups/front-format-ae-r2.GhA6zndn`,
controller SHA256 `fd282bbad2f66e8efa640b5d963139f872dde58f878cfd0f91afb0fbbe6a8544`;
MJPG PNG `5bae61a9fb7b63496cf3b4117f74ed4d3682f09d2378057730b4b9d1b4d7070f`,
HEVC ritorno `56fc827fcb31d9797b60f2def42b8963880425f5ca819fa7724025420d42aa19`,
integrato `2e8e5ca006ab6d0608bc78c5a459eb0d03ac4d5dc30c414b92a648f17e3c47a6`.
Controller consumer integrato SHA256
`4e7819f5b1cf97d2e8c9146e27193ca33b8a435c0a3c54df7a25ffff2db22b47`.
Copie private nella medesima directory workstation `test-board-ae-20260914.N5Rkwvew`.

Decisione dopo due ipotesi controllate: non aggiungere toggle di formato o
policy UVC generiche. Prima distinguere risposta AE a luce controllata, stato
persistente del firmware e modifica della scena. Alla domanda se intorno
alle13:30locali abbia acceso luci o toccato la camera, l'utente risponde **no**.
Priorita' della diagnosi: ricostruire se il recupero segue la riapertura camera
dopo il comando AE0, senza presumere che il semplice readback0 attivi l'ISP.
AE0 resta una modifica diagnostica dichiarata, NON un fix persistito
centralmente. Indagine CPU deployed board esplicitamente sospesa finche' test board non e'
ripristinato o l'utente modifica questa priorita'.

### Verifica successiva dopo la risposta «no» dell'utente

Il14settembre13:29-13:32UTC, senza nuovi SET camera, restart o variazioni
configurazione, rilette otto registrazioni finalizzate e il live del broker.
Cache recovery scaduta rinnovata dal percorso canonico, export34849462418;
nessuna credenziale esposta o copiata in queste prove.

Campione a offset30s nei bucket UTC10:10,10:20,10:30,10:40,10:50,11:00,
11:10,11:20: YAVG33,8032/25,0178/54,9533/66,0745/68,2112/93,5689/94,4202/
92,5122. Nome bucket+offset NON sono un binding esatto alla cattura UTC.
File finalizzati verificati invariati durante lettura; nessun errore FFmpeg.
Ispezione visiva conferma: interni ancora scuri nei primi due campioni,
lampade gia' visibili, successivo aumento di luce diretta/riflessi e movimento
di persone. Il recupero non coincide immediatamente con SET AE0 o riavvio,
quindi non e' una prova causale del ripristino dell'automatismo.

Il live attuale presenta ancora un soggetto molto scuro con finestra/soffitto
visibili: **problema ottico non risolto in modo affidabile**. PNG1920x1080
YAVG35,8016, SHA256
`35d1340edbe4c087d607e1fc618d08a9abcd2529d4aef97f333d3f491a016951`.
270pacchetti pubblici H265, ID107081..107350 contigui,30,028FPS sui timestamp;
peer broker1797832/UID999 autenticato, stessa invocation/root del precedente
readback. La visibilita' migliore delle13:38locali non era sufficiente a
qualificare AE; non viene marcata come fix.

Diagnostica autenticata30s conclusa: configurazione/supervisor invariati,
Tracker16,22-17,84FPS, CPU host26,70-30,02%, Tmax79,197C. Non e' un soak e
non cancella il timeout della verifica precedente. Source ora epoch2/native
1811185: un recupero figlio e' avvenuto fra le letture, non un runtime senza
interruzioni. Nessun approfondimento del consumo deployed board avviato.

Prove private: workstation `test-board-ae-followup.CRGAW4DL`, device
`/var/lib/nemesi/state/operator-backups/front-ae-readback.vzfJh1uL`.
Timeline JSON SHA256 `526d158e7e697b65d9296c705422d72ae472821363d7b5f84d45084f6e04081b`;
script archive SHA256 `ad952531beadd9897c9e0971cbfcfd1456bf6af18b3a713ea63c69337e16ac78`;
script live SHA256 `065fbb0ecb64a817945af00ffdc681f633eea83d7272226b4c3fbb38ac26cbf7`.
Prossimo dato necessario: risposta temporale a una variazione fisica controllata
della luce, per distinguere AE bloccata da misurazione della scena/controluce.
Nessun altro toggle cieco o valore manuale prima di questo confronto.

### Switch integrato front -> overview -> front,14settembre13:54-13:55UTC

Richiesto esplicitamente dall'utente dopo «non cambia molto». Non trattare
questa frase come prova strumentata di copertura lente: non abbiamo una
sequenza luce/buio sincronizzata alla cattura.

Entrambe le camere fisiche presenti: front2bc5:0529 USB1-1.4 `/dev/video2`,
overview2ab8:a101 USB1-1.3 `/dev/video0`. Usata l'API pubblica routing-v2 del
broker, con peer autenticato, preview e CAS; nessun secondo lettore camera,
SET UVC, modifica profili centrali o deploy. Tutti i ruoli spostati insieme,
poi rollback del prodotto all'assegnazione originale. Revisioni103->104->105;
apply concluso13:54:33UTC, rollback13:54:56UTC. Supervisor1797790 e
broker1797832 invariati; root/config verificati dal controller.

Tre campioni live pubblici NMB2,150pacchetti ciascuno, nessuna discontinuita'
di sequence. Decodifica diagnostica offline a un thread, immagini viste:

| Fase | Formato | FPS dai timestamp | YAVG | Esito visivo |
| --- | --- | --- | --- | --- |
| Front prima | H2651920x1080 |30,044|28,8059|Soggetto quasi nero, finestra luminosa|
| Overview | H2651280x720 |25,083|118,542|Interno e soggetto chiaramente leggibili|
| Front dopo | H2651920x1080 |30,020|28,6477|Sostanzialmente invariata, ancora scura|

Il medesimo percorso di prodotto trasmette dunque un'immagine utile da
overview, mentre riaprire front NON ripristina l'esposizione. Le inquadrature
e le ottiche sono differenti: YAVG non e' una misura comparabile dei lux ne'
prova AE/ISP guasta. Il risultato restringe il problema alla front e alla sua
risposta alla scena, non dimostra un oscuramento generale del broker/player.
Non introdotta alcuna cura a posteriori che schiarisca pixel gia' acquisiti.

La prova e' diagnostica, NON un soak o una qualifica overview30FPS: catalogo
richiede30, osservati circa25. Log nativo conferma25,20FPS, nessuna corruzione
o capture timeout a regime nel breve campione. Un timeout iniziale durante
startup e cadenza inizialmente degradata restano nel journal; analysis/archive
rientrano nei target, live circa25 resta sotto27. Non abbassata alcuna soglia.

PAL privato workstation `test-board-source-switch.mbYCLM2f`, device
`/var/lib/nemesi/state/operator-backups/front-source-switch.EzN8depp`.
Controller single-use `switch_trial.py` SHA256
`96669461b1e9a5b3da8fa915efb4102d3304e0091e105c635e1db473bedaf26a`;
unita' `nemesi-front-source-switch-EzN8depp.service` conclusa exit0 e inattiva.
PNG prima/overview/dopo SHA256 rispettivamente:
`66f49b173708c87922fb0d094ee5afc86d80138bdeeb9c582b377801047d2bd7`,
`4c4c3cca6005b812e65271cb22c8a94e0476557c20f87576b3d37eb33411c64a`,
`21b6c934887d826a51780ffd30ffa36753c6030f7d4c730a4222711156c35b6f`.
Nessuna immagine privata in Git. test board riportata su front; deployed board non toccato.

Postflight14:03:35UTC: routing rev105, tutti i ruoli front, live29,996FPS,
analysis19,998/archive14,998, stato autenticato valido/running; CPU81,932C,
DDR81,395C. **Non e' stabilita' continua**: prima lettura respinta per sorgente
initializing fuori da live startup. Journal conferma due interruzioni native
13:58:48 e13:59:01UTC: `interframe_capture_discontinuity` HEVC, sequence
7119->7121 ed253->256, azione `retire_source_before_publication`, release SDK
confermata ed exit64. Recovery automatici media_epoch3->4->5, ultimo native
1830159. Non causati da nuovi comandi operatore; non provata la causa ultima
dei buchi ne' un legame con la sottoesposizione. Il riepilogo perde il motivo
specifico (`child_exit_without_terminal_reason`): collegato alla
[issue diagnostica aperta](2026-09-09-native-frame-send-failure-loses-cause.md).
Log completi delle finestre mirate e `postflight.json` conservati nel PAL
privato sopra; nessuna soglia allentata per far passare il controllo.

### Criteri ancora aperti

#### Tentativo di riavvio richiesto14settembre14:43UTC — esito ancora non verificato

L'utente chiede riavvio ed eventuale spegnimento/riaccensione camera. Hub USB
001:002 su test board dichiara `Ganged power switching` e protezione condivisa;
front su porta4, overview su3 e audio su2. Non eseguito alcun toggle sysfs,
unbind o reset dell'hub: non garantirebbe lo spegnimento della sola frontale.
Preparato quindi il riavvio ordinato di test board autorizzato, non una prova di
rimozione VBUS. Nessun cambio AE/configurazione/profili o nuova release.

Preflight: root/config invariati, servizio abilitato, nessun job concorrente,
misura9358entries/553727013bytes PASS. Avvio confermato del controller single-use
`nemesi-front-reboot-gpR3mzCB.service`: update lock, misura, campione pubblico
prima, quiesce/drain/stop, checkpoint e richiesta reboot; recovery di stop
abortito. Ultimo output osservato14:43:16UTC: unit running,
`baseline_start` alle14:43:10UTC. Dopo, SSH/22 e ping Tailscale senza risposta;
alle14:47:47UTC Tailscale locale online, test board offline. Tentativo LAN192.168.1.3
senza risposta, ma workstation su192.168.77.0/24: non prova di device spento.
Non ancora letto il checkpoint finale o un nuovo boot ID: **non dichiarare
riavvio completato, recovery riuscita o miglioramento ottico**.

PAL device `/var/lib/nemesi/state/operator-backups/front-reboot.gpR3mzCB`,
workstation `<private-workspace>`.
`reboot.py` SHA256
`491f8ef0f83b20366d8aa2924026f8b9999c7fc0985177fe8116281dc31eb39b`,
`capture.py` SHA256
`bbc93b6aeab89e4552bf9b62f1cd4db37c72f262fcff8ecef5c77329cf1dc16d`.
Al ritorno leggere trial/checkpoint PRIMA di agire, verificare boot differente
da5136743a-2c2c-4ccb-b3f3-e5aaaf5d1208 e integrita' root/config, poi confronto
ottico con la stessa sorgente. Non rieseguire il controller o duplicare il
reboot. Richiesta all'utente verifica LED fisici/rete senza togliere alimentazione.

#### Accettazione del ripristino ottico

1. Completare il confronto sorgente/archivio e distinguere sottoesposizione reale,
   stato AE/ISP, metadati colore e trasformazioni successive. Le evidenze del
  12–13settembre comprendono archivio nero e live molto scuro; non bastano
   a dichiarare che oggi AE sia spenta o che il sensore sia guasto.
2. Se serve accesso ai controlli, farlo al confine quiescente del proprietario
   camera, con capabilities/menu reali e recovery verificato; mai un secondo
   lettore o una sequenza di tentativi UVC durante la cattura.
3. Esprimere intento automatico, capacita' dell'adapter e stato effettivo
   separatamente. Inizializzazione/ripristino supportati vanno nel broker,
   non nei client, nel Tracker o nel Registry. Nessun valore fisso manuale
   nascosto sotto la parola auto e nessuna tabella universale da una sola cam.
4. Provare variazioni luce/buio, riavvio e switch, mantenendo cadenza/risorse
   richieste. Post-ISP tone mapping non e' una cura di un'AE bloccata e non
   puo' ricostruire dettagli mai acquisiti; non attivarlo per coprire il difetto.

Non modificati profili centrali, soglie, corpus o firmware.
Il servizio e' stato fermato/ripreso solo per il test controllato descritto.
Il sotto-rilascio resta concluso; il requisito ottico e' ancora aperto.
