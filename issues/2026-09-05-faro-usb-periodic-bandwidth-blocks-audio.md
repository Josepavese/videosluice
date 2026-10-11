# test board: banda periodica USB negata al microfono con due sorgenti attive

Stato: fault storico riprodotto; audio corrente ripreso dopo il deploy
`1e296ff0...`. Sostenibilita' di tutte le combinazioni USB ancora da qualificare.

## Evidenza osservata

Il 2026-09-05, dopo il deploy ufficiale `cd0d4874...`, test board conserva il piano
56 FOO: live sulla frontale, analysis/archive sulla panoramica. Il recorder
scrive video, ma l'ultimo frame audio risale alle 20:12:03 Europe/Rome.
Alle 20:12:07 il kernel riporta per `usb 1-1.2`:

```
Not enough bandwidth for new device state.
Not enough bandwidth for altsetting 1
2:1: usb_set_interface failed (-28)
```

ALSA rifiuta l'installazione dei parametri S16_LE/mono/48000 Hz; il broker
audio riprova con backoff fino a 30 s. Il processo resta attivo e pubblica
status, ma questo non significa che consegni audio: il campione canonico
fallisce correttamente con `recorder audio input is stale or future-dated`.

Topologia letta da kernel/ALSA e `lsusb -t`:

- microfono C-Media `1-1.2`, full speed 12 Mbit/s;
- camera `1-1.3`, high speed 480 Mbit/s;
- camera `1-1.4`, high speed 480 Mbit/s;
- tutti sul ramo USB2 dello stesso hub/controller; il ramo USB3 separato
  mostrato dal sistema non rende automaticamente SuperSpeed queste periferiche.

L'errore riguarda l'ammissione USB, non la dimensione del file recorder o la
capacita' libera della SD. Non prova un leak, una SD difettosa o una modifica
di power management. Non equivale nemmeno a una misura del bitrate video:
la banda periodica riservata dal controller e il payload H265 sono distinti.

## Recovery e confini di prova

Aggiornamento22:43-23:06: dopo il riavvio applicativo ufficiale il campione
completo passa anche con piano56 FOO. Passano poi i campioni durante il giro
OOO->FOO->OOO a zero viewer e sul successivo piano60 FOO; audio/video recorder
avanzano e una finalizzazione termina senza errori. Questo dimostra recovery
e continuita' nel periodo osservato, non una modifica al driver USB o la
rimozione universale del limite di banda. La qualificazione della release
si ferma per il distinto difetto di generazione live dopo sostituzione encoder.

Un tentativo autenticato/CAS di portare il piano a OOO, ritirando la frontale,
fallisce in verify per il distinto difetto bootstrap del candidate. Rollback
automatico, nessun apply forzato: non affermare che abbia liberato il bus o
recuperato l'audio. Il successore `1e296ff0...` contiene la correzione H26x
encoder-owned, non una modifica al driver USB o al microfono.

Dopo il suo deploy verificare il ritiro effettivo della frontale e la ripresa
autonoma dei frame audio, poi ripetere il campione completo e il soak. Se OOO
recupera l'audio, questo dimostra il recovery in quel piano, non la sostenibilita'
di tutte le combinazioni a due camere sul medesimo ramo USB.

Non disabilitare audio/autoesposizione, abbassare soglie di test, introdurre
limiti globali al numero di camere, modificare quirk kernel o resettare USB
senza una prova specifica. Un futuro controllo di ammissione delle risorse USB
deve riferirsi alla topologia/capacita' reali e ai consumer concorrenti, non a
un'eccezione hardcoded per test board o a supposizioni tratte da questo solo hardware.

Evidenze locali, senza audio/video privati:
`/tmp/nemesi-test-board-live-control-release.NYx1nVkk/cd0d-audio-origin.log`,
`cd0d-audio-kernel-and-alsa.log`, `cd0d-audio-stale-health.jsonl`,
`cd0d-recovery-terminal.jsonl`. Dettagli del fix e relativo stato nella
[issue del controllo live](closed/2026-09-05-live-control-not-rearmed-after-fanout-switch.md).
