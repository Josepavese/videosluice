# test board: disconnessioni USB delle camere, identita' e cause da distinguere

Stato: **aperta, sintomo noto; causa non dimostrata**. Non e' una diagnosi di
camera difettosa e non e' una limitazione generica del numero di sorgenti.

## Episodio distinto14settembre09:33 locale: live overview rifiutata per banda USB

Recupero read-only richiesto dall'utente; nessuno switch ripetuto, deploy o
modifica dispositivo durante questa indagine. Non confondere questo episodio
con le disconnessioni fisiche del9settembre, la cui causa resta aperta.

- Audit webserver07:33:13UTC: preview accettata, revision103. Alle07:33:15UTC
  apply accettato come operazione asincrona
  `web-1789371195401412488-5`, stato running, NON successo definitivo.
- Candidato overview1280x720H265@30, solo ruolo live (`roles=live/-/-`), mentre
  il worker front1738424 continuava a catturare: riepilogo09:34:08locale
  1800frame/60s,30FPS, analysis1200/archive900. Era quindi un tentativo di
  cattura simultanea, diverso dalla prova pomeridiana che spostava tutti i
  ruoli e chiudeva front prima di avviare overview.
- Tutti i5avvii overview falliscono prima di produrre frame:
  `VIDIOC_STREAMON failed err=No space left on device`, alle09:33:17,
  09:33:22,09:33:33,09:33:54,09:34:24 Europe/Rome.
- Kernel correlato a ogni tentativo, porta overview1-1.3:
  `Not enough bandwidth for new device state.` e
  `Not enough bandwidth for altsetting 15`.
  E' un rifiuto della prenotazione di banda USB, NON spazio SD esaurito.
  La [documentazione kernel ENOSPC](https://cdn.kernel.org/doc/html/latest/driver-api/usb/error-codes.html)
  conferma la semantica per i trasferimenti USB periodici.
- Recovery classificato `uvc_producer_handoff`,5retry con attese crescenti,
  senza reset device; la condizione rimane. Alle09:34:45locale, dopo90s,
  `routing apply phase=admission: ... source="overview" fresh_frame=false
  child_pid=0 media_epoch=5 ... context deadline exceeded`; terminale
  `state=failed phase=abort code=apply_failed`. Rev103 non promossa.
  I rifiuti `busy` dei read concorrenti sono effetti dell'operazione in corso,
  non la causa iniziale del fallimento.

La prova pomeridiana single-source overview riuscita NON qualifica quindi
questa assegnazione parziale/contemporanea. Nessuna diagnosi generale di
camera guasta, saturazione CPU o impossibilita' multicamera su altre topologie.
Restano da valutare la prenotazione UVC effettiva/topologia e il messaggio
operatore: il motivo USB concreto dovrebbe emergere, senza nascondersi dietro
un generico timeout o un retry di handoff. Nessuna correttiva implementata qui.

Prove private persistenti in
`<private-workspace>`:
`morning-switch-details.log` SHA256
`638863976cd1a33ceaa57e9430b36fb3f89d4509b2d9563b59362f4fa97e083a`,
`morning-switch-kernel.log` SHA256
`e36b54be135a94d91511a5050eb69786656cfdc83952cbe41b2dd1a7243f322b`,
`morning-routing-audit.jsonl` SHA256
`513c481d13ef9bd4824e8a7d2b3ef9d63d95112134f1d6a3d09de8950ab63955`.
Audit esportato solo con timestamp/event/esito/dettagli routing, senza identita'
operatore, indirizzi client o credenziali. deployed board non consultato/modificato.

## Lettura10settembre23:12UTC

Enumerazione read-only durante la promozione Tracker, nessun reset/cambio USB:
front2bc5:0529 nuovamente presente su1-1.4 con by-id
`usb-webcamvendor_webcamproduct_00000000-video-index0` -> video2;
overview2ab8:a101 su1-1.3, `usb-Linux_Foundation_Webcam_gadget-video-index0`
-> video0. L'ultima lettura che riportava front assente e' quindi storica,
non lo stato corrente. Non ricostruito l'istante/cause del ritorno, ne'
qualificata l'apertura o la stabilita' della cattura dalla sola enumerazione.
Nessuno switch effettuato durante il deploy o il confronto FPS precedente.

## Episodio verificato del 9 settembre

Binding del profilo e enumerazione Linux, non nomi dedotti dal prezzo:

| Sorgente configurata | USB VID:PID | Porta osservata | Descrizione USB |
| --- | --- | --- | --- |
| front | 2bc5:0529 | 1-1.4 | webcamvendor / webcamproduct; nome commerciale non identificato |
| overview | 2ab8:a101 | 1-1.3 | Linux Foundation / Webcam gadget; nome commerciale non identificato |

- Alle14:11:11UTC (16:11:11 Europe/Rome), kernel: `usb 1-1.4: USB disconnect,
  device number 5`. La front scompare anche da lsusb/sysfs; non e' solo uno
  stream fermo o un alias /dev obsoleto.
- Il candidato d3725c41 non era attivo: Python456s/compile777s PASS, poi trial
  rifiutato per `matches=0 vidpid=2bc5:0529`. Nessun test FAP su d372 eseguito.
- Porta4: disable=0, runtime_status=active, over_current_count=0. Hub attivo,
  power/control=auto. Queste letture non provano ne' escludono guasti elettrici
  o una precedente transizione energetica della periferica ormai scomparsa.
- Alle14:29:18UTC la stessa VID:PID viene enumerata sulla stessa porta1-1.4,
  device number6. Nessun reset USB, reboot, cambio porta o modifica del power
  management e' stato eseguito dall'agente. Eventuale intervento fisico esterno
  non verificato: non chiamare questo ritorno spontaneo.
- La baseline9ffc riprende automaticamente la front e il Tracker: alle14:31UTC
  frame avanzanti, read_errors0, circa71C; recovery postflight15s e accesso PASS.
  Deploy d372 termina fallito con precedente release ripristinata. Non e' un
  soak, una prova di affidabilita' della camera o la qualifica del nuovo codice.
- Misura esatta della release9ffc dopo il recovery: PASS9262entry/552700084byte;
  nessun albero firmato modificato manualmente.

## Secondo episodio: USB tornata, recupero software bloccato

Il9settembre alle15:14:49UTC la front2bc5:0529 si disconnette nuovamente,
questa volta mentre e' attiva la derivata Tracker su baseline9ffc. Il kernel
riporta URB ENODEV; il worker66679 fallisce DQBUF con `uvc_dqbuf_enodev`.
Alle15:14:50UTC la stessa identita' torna sulla stessa porta1-1.4 come device7.
Nessun comando USB/reboot/switch dell'agente nella finestra; eventuale
intervento fisico esterno non confermato. La causa della disconnessione resta
non dimostrata, separata dal seguente difetto software riprodotto.

Il worker chiude4/4mapping e il descrittore con successo, ma STREAMOFF sul
dispositivo rimosso restituisce ENODEV19. `close_v4l2_capture` considerava anche
questo caso rilascio incerto; il worker usciva65 e l'autorita' FAP tratteneva
correttamente l'ammissione con `resource lifecycle not confirmed: release_failed`.
I tentativi successivi non potevano ripartire, nonostante la USB fosse tornata.
L'overview continuava a registrare; il Tracker e' uscito alle15:14:51UTC e il
supervisor attendeva un nuovo binding. Il suo ultimo tracker_status restava
visibile ma era stale, non una prova di Tracker ancora in esecuzione.

Correttiva locale: riconoscere esclusivamente ENODEV di STREAMOFF come
`streamoff_outcome=device_removed`, richiedendo ancora munmap e close riusciti.
EIO/EINVAL/EBADF/ENXIO e qualsiasi errore di rilascio encoder/decoder restano
negativi. Nessuna accettazione generica dell'exit65 e nessuna quota liberata
soltanto perche' il processo e' morto. Il contratto esistente64 mantiene il
fallimento della cattura distinguendolo dal rilascio verificato delle risorse.
Semantica supportata dalla documentazione Linux: [ENODEV](https://cdn.kernel.org/doc/html/latest/userspace-api/media/gen-errors.html)
e [V4L2 close](https://docs.kernel.org/5.16/userspace-api/media/v4l/func-close.html).

Regressione sulla vera funzione C: prima RED sul caso ENODEV con unmap/close
riusciti; dopo40combinazioni di errori e controlli di chiusura ripetuta verdi
sotto ASan/UBSan, suite native media/fanout PASS. Test Go source/framedomain
race count1 PASS, mantenendo il rifiuto di rilascio non attestato. Non e' ancora
una prova di recovery hardware sul device corretto; issue tuttora aperta.
Letture dopo il guasto: circa73C, USB presente ma runtime suspended dopo la
chiusura del capture; quel campione non dimostra autosuspend come causa.

Evidenze aggiuntive: `onboard-front-second-failure.log`,
`onboard-single-front-degraded-diagnostic.log`, `uvc-disconnect-release-red.log`,
`uvc-disconnect-release-green.log`, `uvc-disconnect-release-go-receipt-fresh.log`
nella directory temporanea indicata sotto.

## Terzo episodio: enumerazione USB fallita dopo il riavvio della derivata corretta

Il sync canonico della correttiva UVC termina con successo alle15:54:51UTC;
la derivata c8ca34ec contiene il nuovo nativo763fe4db e gli hotfix Tracker.
Misura esatta PASS9267entry/552783604byte. Il successo del sync non equivale
al postflight: quest'ultimo termina fallito per broker degraded/front assente.

Alle15:54:57UTC il kernel avvia un reset di1-1.4/device7, seguito da ripetuti
`device descriptor read/64, error -110` e `device not accepting address,
error -62`. Alle15:56:00 disconnette il device7; falliscono anche le nuove
enumerazioni8/9/10/11 e il tentativo power cycle del kernel. Alle15:57:05:
`unable to enumerate USB device`. Al controllo16:03UTC front2bc5:0529 e'
assente da lsusb; overview2ab8:a101 presente e archive circa15fps.
Supervisor115388, broker115458 e worker overview115797 attivi; NRestarts0,
Tracker non avviato (nessun input ammesso), temperature circa72.4-72.7C.

L'avvio front e' fallito durante il probe, prima di creare il worker nativo;
non e' una ricorrenza dimostrata del rilascio STREAMOFF/ENODEV appena corretto.
Il broker ritenta con backoff fino60s e rifiuta correttamente una periferica
diversa. Nessun apply routing: piano96 invariato, single-front ancora non
attivato. Nessun reset USB, power-cycle, modifica PM o reboot impartito
dall'agente; distinguere i tentativi automatici registrati dal kernel.
L'adiacenza al riavvio/probe e il precedente campione runtime suspended sono
indizi temporali, non una diagnosi di autosuspend o di camera guasta.

Richiesto un ricollegamento fisico della sola front per recuperarne
l'enumerazione. Non resettare l'hub condiviso con altre periferiche. La
correttiva software resta distribuita come sviluppo, non qualificata come
recovery hardware o release firmata. Questo aggiornamento documentale e'
successivo al payload misurato; non richiede un nuovo deploy.

Evidenze nella directory temporanea sotto: `uvc-development-first-identity.log`,
`uvc-development-postflight.log`, `uvc-development-runtime-diagnostic.log`,
`uvc-development-front-failure-origin.log`, `uvc-development-usb-start.log`,
`uvc-development-port-diagnostic.log`.

## Segnalazione storica dell'utente, separata

L'utente riferisce che una camera, ritenuta inizialmente overview, da tempo
scompare e non torna neanche dopo reboot del device; cambiarle porta USB la
rende nuovamente visibile. Ha poi precisato che, se fosse la front piu' costosa,
la valutazione sarebbe diversa. Non attribuire quel difetto alla front senza
associare il modello fisico alla VID:PID. Non fonderlo con l'episodio sopra,
che comprende una nuova enumerazione sulla medesima porta.

## Mitigazione e accettazione residua

L'utente ha autorizzato temporaneamente il lavoro con una sola camera. Usare
selezioni esplicite e verificate, preservando catalogo e dati della camera non
usata; non introdurre fallback impliciti o eliminare la camera come guasta.
L'eventuale qualifica single-camera non chiude la matrice multi-source FAP.

Prima di dichiarare la causa: confermare modello/VID:PID/porta, raccogliere eventi
USB e stato del runtime, distinguere periferica/cavo/hub/alimentazione/PM.
Non applicare quirk, disattivare protezioni o aumentare soglie per supposizione.
Verificare recovery, binding alla stessa identita', epoch non riusabili e
consumatori corretti dopo un hotplug reale; nessuna sostituzione cross-camera.

Evidenze locali persistenti: `<private-workspace>`
`d372-uvc-failure-readonly.log`, `d372-usb-binding-power.log`,
`d372-camera-model-readonly.log`, `d372-front-reattached-readonly.log`,
`cadence-registry-deploy.log`. Nessuna immagine o credenziale esportata.
