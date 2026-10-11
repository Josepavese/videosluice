# Generazione live riutilizzata dopo la sostituzione dell'encoder

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. La generazione pubblica appartiene al capture e non riparte sostituendo l'encoder.

Evidenza: Regressione reale V4L2 e sanitizer; epoch 1,2,3,4 osservate su test board 1ecd e due consumer 2850 s con soak verde.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


Stato: corretto e verificato su HW test board `1ecd1414...`; soak2700s e due
consumer2850s conclusi con esito positivo il 2026-09-06.
Nessuna promozione deployed board ancora eseguita per questo successore.

## Prova reale

Il deploy ufficiale `1e296ff0...` termina alle 22:42 del 2026-09-05:
healthy/applied, misura esatta 8920 entry/547851044 byte, token e preset test board
dual v7 preservati. Agent PID1644543, invocation
`962763aa1a14431b9c3bc91e36288b3d`, NRestarts0.

Il primo FOO->OOO termina in 0,842 s, piano57. Il campione precedente aveva
zero viewer, ma il journal attesta un peer WebRTC gia' connesso dalle22:45:17:
questa prova vale CON spettatore, non a zero. Un'anteprima iniziale con ID
che iniziava con una cifra era stata respinta; corretto soltanto il probe,
nessuna mutazione inviata con quell'ID e nessun contratto di prodotto allentato.

Dopo lo stop del player confermato dall'utente, le richieste con guard esplicito
clients=0 portano OOO->FOO in6,363s (piano58), poi FOO->OOO in1,200s (piano59).
Overview PID1644984/epoch1, analysis generation12 e archive generation25
rimangono invariati. La misura idle di43,629s conferma zero nuovi input/output
VPU live, mentre l'archive avanza di656 frame; tracker, audio e video recorder
avanzano senza drop/errori/restart. La decodifica di un campione H2651280x720
e' riuscita sul PC, con media trasferiti soltanto in pipe e nessuna immagine
salvata. Non equivale a una nuova prova browser end-to-end.

Alle22:52 due consumer simultanei, dopo il park, falliscono entrambi sulla
prima lettura entro5s. Non aumentare la deadline. Il worker attesta
`activation_changed=true`, emette frame e pubblica control-ready seq24 con
encoder_epoch2, ma il broker ha gia' osservato2. `mainAwaitingEncoderEpoch`
rimane quindi attivo: gli access unit vengono scartati prima del gate header.

## Causa e correzione

Il contatore dell'encoder apparteneva all'istanza hardware, quello osservato
dal broker all'intero capture. Sostituire il ramo crea un encoder che riparte
da1: la sequenza pubblicata poteva essere1,2,1,2. La cache H26x della release
precedente corregge gli header, non questa collisione di generazioni.

`native/stream_epoch.h` definisce ora un clock di pubblicazione generico,
senza dipendenze Nemesi/codec/hardware. Il worker conserva il clock fuori dalle
istanze sostituibili; cutover/rollback live invalidano il binding, non il
contatore pubblico. Soltanto un access unit effettivamente pubblicato avanza
la generazione e il marker precede i frame. Warmup privato, rate change,
client aggiuntivi e modifiche ai soli altri ruoli non creano epoch. Overflow
e regressione privata senza cambio owner falliscono chiusi.

Il control-ready usa lo stesso contatore pubblico del marker binario.
Nessuna compatibilita' runtime, modifica driver, soglia termica, reset device
o fallback software. Il worker V4L2 condiviso copre le sue sorgenti H26x,
MJPG/JPU e raw; non si dichiara una nuova modalita' dinamica per backend che
non la attestano. Le suite HBN restano verdi.

## Test e rilascio

La regressione include il codice reale di pubblicazione V4L2: prima fallisce
su `read_epoch(wire)==3`, poi verifica1,2,3,4. Esegue inoltre i callback reali
di cutover/rollback/retire/abort, con H264 e H265, riuso della generazione
privata prima di un frame dell'incumbent, preservazione del live durante
modifiche analysis, rate change, input invalidi e overflow. Nessuna API
hardware viene aperta nel test host. ASAN/UBSAN/LeakSanitizer verdi; suite
native, contratto V4L2 e runtime HBN verdi.

Non qualificare `1e296ff0...` per la promozione deployed board: serve il successore
firmato e la ripetizione di switch/park/resume/due consumer, quindi soak
canonico >=2700s. Nessun soak avviato su questo artefatto fallito.

Evidenze in `/tmp/nemesi-test-board-live-control-release.NYx1nVkk/`:
`1e296-return-{apply,progress}.jsonl`, `1e296-idle-comparison.json`,
`1e296-client-{a,b}.{jsonl,err}`, `1e296-resume-timeout-events.log`,
`1e296-epoch-replacement-red.log`, `1e296-epoch-final-green.log`,
`1e296-epoch-sanitizer.log` e log delle suite.

Durante i test locali un controllo successivo trova piano60 FOO, modifica
non eseguita da questo audit. Preservare l'intento corrente (live frontale),
non ripristinare automaticamente il precedente piano59 OOO. Agent e overview
restano invariati, recorder writing e prima finalizzazione riuscita, zero
drop/errori; DDR81,297/CPU80,662C nel campione delle23:05-23:06.

## Verifica del successore, 2026-09-06

Deploy ufficiale concluso verso le00:10: artefatto
`1ecd14140cc264a119859ac3bccec53059b8ac8e484f2f665a87cdda8d4db61c`,
sequenza `1788642885663763855`, codice `c20c84dfadce45ac7b746a091bafa81a1f905212`.
Misura indipendente8924entry/547870734byte, ripetuta dopo i probe con lo
stesso esito. Healthy/applied, token e preset centrale dualv7 preservati.
AgentPID1731776/invocation `71388c8e72a0454c8e492679dfbe1294`, NRestarts0;
overviewPID1732217/epoch1 conservato per tutte le prove.

- Switch guarded a zero viewer FOO->OOO0,885s, OOO->FOO6,231s,
  FOO->OOO1,194s: piano60->63, solo generation live36->39;
  analysis12/archive25, endpoint, processi e contatori di errore invariati.
- Tre coppie di consumer, rispettivamente90/90/75s per consumer: bootstrap
  H265 completo1280x720, primo frame in152/1,5ms,122/1,6ms,82/1,6ms;
  gap massimo60,1ms. Generazioni pubbliche osservate1,2,3,4, senza regressione
  o restart del capture, comprese le riaperture dopo sostituzione encoder.
- Coppie HW valide sotto carico: live+1162/archive+697 frame in46,408s,
  poi live+396/archive+239 in15,856s. Una lettura intermedia del secondo
  ciclo arriva DOPO il termine dei consumer: fallisce correttamente solo
  `after_clients` e `after_live_active`; non viene conteggiata come prova
  sotto carico. Il terzo ciclo usa una temporizzazione esplicita, non soglie
  piu' permissive.
- Park: live input/output+0 e archive+687 in45,780s, poi+246 in16,396s,
  infine+253 in16,890s. Tracker e recorder audio/video avanzano senza nuovi
  drop/errori. Campione H265 realmente decodificato sul PC in pipe, senza
  salvare media e senza introdurre decoder software sul device.

Il target VPU e'30fps/4096kbps, ma il video misurato e'25,026fps: anche
l'acquisizione nativa a live spento e' circa25fps. Non qualificare30fps e
non confondere l'eta' del timestamp sul socket (~2ms p95) con la latenza
ottica end-to-end o la presentazione browser. La cadenza della camera e'
tracciata nella [issue separata](../2026-09-06-overview-capture-25fps-under-30fps-request.md).

Alle00:31:44CEST avviato soak canonico2700s con due consumer autonomi2850s:
`nemesi-operator-video-soak-1ecd-test-board.service`, invocation
`108b8fbbdd744c1dbc2118b5117de248`; consumer
`nemesi-operator-live-1ecd-test-board-{a,b}.service`. Il minimo richiesto dal runtime
e'1260s, ma si mantiene il requisito piu' forte del goal2700s.

Concluso alle01:16:45CEST: report v3 root0600
`95c735ff66bc784e6e250b0202377358.json`, `passed`,2700,118s/5401campioni,
massimo83,447C, zero overrun, stessa release/sequence/agent verificati.
MP4video+40517/audio+134920, quattro finalizzazioni, zero drop raw/archive
o errori di finalize/retention. I checkpoint storage lenti restano diagnostica
(16 durante la prova, massimo sync7705ms), non vengono nascosti dal pass.

Entrambi i consumer completano2850s alle01:18:09CEST, coprendo l'intero soak:
71324frame ciascuno,25,026fps, primo frame74,4/56,8ms, gap massimo62,9ms,
header completi e stesso endpoint. Park successivo16,538s: VPUlive input/output
+0, archive+248, audio/tracker in avanzamento e zero nuovi errori/restart.

Ripristinata la scelta operatore con CAS63->64FOO alle01:21:37CEST, sola
livegen39->40, analysis12/archive25 e overviewPID1732217/epoch1 preservati.
Operation `r1ecd-restore-front-apply-20260906`,13,624s. Postflight indipendente,
continuita' audio/video/tracker per ulteriori56,689s, misura esatta
8924entry/547870734byte, token e preset invariati. Tutte le unita' di test
sono terminate. La frontale passthrough resta una sorgente statica: queste
prove di park riguardano il VPU controllabile, non un controllo inesistente
dell'encoder interno USB. Nessuna nuova prova browser end-to-end dichiarata.

Prove `1ecd-soak-report-verified.jsonl`, `1ecd-loaded-soak-comparison.json`,
`1ecd-post-soak-idle-comparison.json`, `1ecd-restore-front-{progress,comparison}`,
`1ecd-restored-front-stability.json` e `1ecd-after-soak-restore-*` nello stesso
root audit. Nessuna promozione deployed board o certificazione di tutte le combinazioni
fisiche/HW implicita in questo esito.
