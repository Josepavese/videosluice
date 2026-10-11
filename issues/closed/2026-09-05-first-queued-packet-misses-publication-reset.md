# Il primo pacchetto accodato puo` perdere il marcatore di nuova pubblicazione

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. Il primo pacchetto consegnato porta il reset anche quando il bootstrap arriva dalla coda.

Evidenza: Replay fe23695d a otto transazioni, due consumer, fault/recovery test board e soak test board/deployed board verdi; qualifica browser generale distinta.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Stato: corretta e distribuita su test board e deployed board; replay, soak di entrambi e fault/recovery test board verdi; matrice browser generale distinta e ancora aperta.
- Componenti: broker encoded fan-out, consumer recorder; generica per backend.
- Rilevata: 2026-09-05 durante la matrice test board sull'artefatto `6fd73a47486b542b...`.
- Correlata: [ammissione delle sorgenti lente](../2026-09-05-explicit-source-rejected-for-low-fps.md).

## Evidenza sul dispositivo

La transazione `matrix-apply-1788569992136430340`, dalle 02:59:52 alle
02:59:54 Europe/Rome, ha applicato `FOO -> OOF` (ordine live/analysis/archive)
e persistito la revisione 41. La live e` passata a overview, archive a front;
analysis e il suo capture owner overview non sono stati riavviati.

Durante il cambio il recorder ha segnalato:

```text
video broker discontinuity previous_frame_id=68 current_frame_id=169 missing_frames=100 epoch_changed=false
```

Il collaudo si e` fermato su `recorder archive is degraded`. L'operazione del
broker era invece terminata `succeeded`: e` stata interrogata per ID, senza
ripetere la mutazione. Il recorder e` tornato `writing` al segmento seguente,
ma il contatore `archive_dropped_total=100` resta diagnostica dell'incidente,
non una prova che tra due sorgenti esistessero realmente quei 100 frame persi.

Non sono stati cambiati timeout o soglie, cancellati record, riavviati servizi
o eseguiti hotfix sul device. Dopo la diagnosi e` stato ripristinato il piano
front-live/overview-analysis/overview-archive, revisione 42, con l'API runtime.

## Causa e correzione

`brokerClient.writeLoopWithBootstrap` aggiungeva `FlagStreamReset` soltanto
all'elemento zero della cache di bootstrap. Il cutover pubblica il nuovo
socket prima che i successivi frame abbiano necessariamente riempito quella
cache. Un consumer che si collega nella finestra riceve una cache vuota e poi
il bootstrap dalla coda ordinaria, senza il marcatore di nuova pubblicazione.
Il recorder puo` cosi` confrontare due contatori source-local non confrontabili.

La regressione deterministica collega il client con cache vuota e accoda
bootstrap piu` frame successivo. Prima della correzione fallisce in tutte le
combinazioni live/archive e H264/H265. Il writer ora marca il primo pacchetto
effettivamente consegnato, indipendentemente da cache o coda, una sola volta.
Il buffer condiviso resta immutabile; soltanto quel primo pacchetto viene
copiato. Il percorso NV12 non riceve il marcatore encoded.

Non viene cambiato il wire format, ne` il recorder viene reso permissivo:
bootstrap codec, perdite nello stesso flusso, capienza delle code, freschezza e
autenticazione restano verificati. La correzione e` nel fan-out comune e vale
per tutti i provider e acceleratori che lo utilizzano, non soltanto RDK X5.

## Verifica e limite di rilascio

- Regressione riprodotta prima della modifica e verde dopo.
- Suite completa broker e recorder verdi.
- Regressioni bootstrap/queue con race detector, ripetute 20 volte, verdi.
- Qualificazione finale: browser multi-client ancora necessario. Il soak
  >=2700 s e` superato; replay e recupero sul successore sono descritti sotto.
  deployed board resta escluso fino al completamento dei gate test board.

## Replay sul successore firmato

L'artefatto `fe23695db9cb07aed5523c555d4d3ad9aaf0d1952464eac61d3fe6a9d75d4f8e`
e` installato e misurato esattamente su test board. La matrice ripetuta il 2026-09-05
ha completato otto transazioni (rev. 42 -> 50), compresa `FOO -> OOF`, con
invarianti degli stream non coinvolti e contatori drop/finalizzazione errori
recorder invariati. Due consumer simultanei live/archive hanno verificato per
45 s reset iniziale, VPS/SPS/PPS+IDR e continuita` dei timestamp senza EOF.

Il recorder era ancora nella finestra idle precedente al segmento allineato:
la matrice non certifica segmenti finalizzati. Il soak canonico di 2.700 s e`
stato avviato alle 04:07:48 Europe/Rome nell'unita`
`nemesi-operator-video-soak-fe23695d-test-board`, con due consumer SDK live aggiuntivi
per il fan-out. Il suo esito finale e` descritto sotto; il gate browser resta
aperto, senza promozione deployed board. Evidenze del replay:
`/tmp/nemesi-test-board-fe236-qualification.es1ynLwp/`.

Evidenze operative fuori dal repository:
`/tmp/nemesi-test-board-switch.7mYUSVwb/` (matrice, stato autenticato, journal mirato,
regressione prima/dopo e suite). Nessun dato video e` stato salvato dai probe.

## Soak concluso e bootstrap dopo recupero

Il report canonico v3 `2321316e1e1990b1c5ea57593983fca1` e` `passed`:
2.700,120 s e 5.398 campioni, terminato il 2026-09-05 alle 04:52:50
Europe/Rome. Artifact/sequence e identita` del servizio sono verificati;
quattro finalizzazioni archive senza drop o errori e nessun restart agent.
Due consumer live simultanei da 2.850 s hanno ricevuto oltre 30.800 frame
ciascuno, con bootstrap H265 completo e senza EOF.

Dopo la conclusione, un solo SIGTERM alla cattura frontale ha provocato il
recupero della sola source: PID `165462` -> `256927`, epoch 1 -> 2. Il primo
osservatore temporaneo si e` fermato sullo stato transitorio `degraded`; i
controlli successivi read-only hanno verificato identita` kernel e invarianti,
senza ripetere il guasto o indebolire il validatore canonico. Un consumer
archive continuo ha attraversato la prova senza EOF/drop (gap massimo
100,022 ms). Un nuovo consumer live ha poi verificato reset iniziale,
VPS/SPS/PPS+IDR e 500 frame in 45 s senza EOF, gap massimo 99,011 ms.

Questi risultati qualificano broker/recorder sullo stesso artefatto, non
rendering o riconnessione WebRTC nei browser. La live frontale produce circa
10,8 fps su 25 richiesti: la prestazione nominale resta non qualificata.

## Stato corrente dopo autorizzazione deployed board

I vincoli deployed board riportati sopra descrivono la fase precedente. Il 2026-09-05
l'utente ha accettato la qualificazione test board e autorizzato esplicitamente la
promozione invariata dello stesso ZIP `fe23695d...`. Trial, postflight, misura
esatta e finalizzazione A/B deployed board sono riusciti; stato `healthy`/`applied`.
Il controllo browser leggero deployed board conferma H265 1280x720@20 e rendering;
non sostituisce la matrice generale browser durante fault/cutover.
Il soak deployed board da 2700 s, avviato alle 10:28:22, e` terminato `passed` alle
11:13:23: report `816fb48668cfa368eff73abf34c6d8f0`, 5400 campioni, quattro
finalizzazioni archive senza drop/errori e identita` runtime invariata. Il
[storico del goal](../../../../improvements/HISTORY_VIDEO_BROKER_V3_2026-09-06.md)
mantiene separati risultati provati e prove ancora aperte.
