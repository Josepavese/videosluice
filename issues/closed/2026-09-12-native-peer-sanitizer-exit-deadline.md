# Native peer: distinguere scadenza funzionale e uscita dei sanitizer

Stato: **CHIUSO, 12 settembre 2026, prova ARM conclusa 14:36 UTC**.
Assegnazione: **N0f/N6, collaudo finale del goal corrente**.
Correzione della fixture, non una regressione video o un leak runtime dimostrato.

## Risultato e causa verificati

Il marcatore `NATIVE_MAIN_RETURN handles_released=1` viene verificato entro
la deadline funzionale originale di5s, dopo bootstrap/scambio/rifiuti e
rilascio degli handle. test board mostra poi il processo in `ptrace_stop.part.0`,
stato `tracing stop` e `TracerPid` non nullo durante l'uscita dei sanitizer;
la successiva uscita e' zero. Tutti18casi hanno questa evidenza distinta.
Il vecchio `cmd.Wait()` condivideva il timer5s e terminava questa fase finale.

Anche il child Go del caso `foreign_sender` ereditava quel timer e veniva
atteso in defer, dopo il sanitizer: il rifiuto del mittente funzionava, ma
Wait riportava `context deadline exceeded`. Ora solo la vita dei processi
usa il contesto separato e bounded30s; anche `SENT` resta verificato sotto5s.
Un marcatore tardivo/incompleto non e' PASS, e il marcatore finale non
sostituisce mai il reap con exit0. ASan/UBSan e leak detection non disabilitati.

La stessa fixture C viene compilata una volta per invocazione del package,
con directory privata rimossa da TestMain; ciascun caso avvia un processo
sanitizzato indipendente. Nessun riuso di artefatti di provenienza ignota.
Il controller acquisisce margine prima del test e non usa SIGSTOP: compilazione
sotto86C, qualifica sotto90C, senza modificare le soglie del dispositivo.

- Package locale PASS0,809s; versione finale anche race PASS5,972s.
- Package ARM: **64casi PASS, 110,699s**,18uscite native zero; Tmax86,427C.
- `TestWorkerGroupKernel` e' l'unico SKIP, fixture cgroup delegata opt-in:
  questa prova non qualifica quel gate kernel separato ne' tutto N6.
- Hash dei17sorgenti di produzione coinvolti coincidenti col runtime.
  Supervisor1160246/NRestarts0, configurazioni e release invariate;
  misura esatta finale PASS. Nessun deploy, riavvio o modifica dei dati.

Prova terminale: `nemesi-native-peer-phase-AM6i8Pmq-r5.service`, exit0.
PAL test board: `/var/lib/nemesi/state/operator-backups/native-peer-phase-AM6i8Pmq/run-r5/`.
Copia workstation persistente:
`<private-workspace>`.
`qualification-result.json`, `arm-package-tests.log`, log race e misura finale.
Le precedenti prove invalide per margine termico e il fallimento del child Go
restano nel PAL, non sono state rinominate come PASS. Nessun nuovo deploy e'
necessario per questa correzione dei test; runtime immutato.

## Evidenza del 12 settembre

Su test board, la suite Go ARM del candidato crypto-startup ha superato il package
`cmd/video-broker` in 71,758 s prima della prima pausa termica. Nel package
`framepeer/provider/linux`, che non e' stato modificato, falliscono
`TestNativeLaunchInterop` e `TestNativeLaunchRejectsInvalidBootstrap`:
diversi processi nativi vengono terminati dalla scadenza complessiva di 5 s
durante `cmd.Wait()`, dopo le verifiche funzionali precedenti. Questo e'
compatibile con il ritardo di uscita ASan gia' osservato nella fixture capture,
ma va confermato raccogliendo marcatore finale, stato e uscita del processo.

Inoltre il controller diagnostico ha sospeso il gruppo per raffreddamento
86/83 C tra 04:18:51 e 04:20:56 UTC: il subtest foreign_sender dura 127,75 s
e scade. I timer monotonic continuano durante SIGSTOP. Non attribuire tutti
i fallimenti alla pausa: diversi casi da 5,01 s avvengono separatamente.

Prove persistenti:
`/var/lib/nemesi/state/operator-backups/crypto-startup-0n4ZS1Pf/go-tests.log`
e `build-controller.log` su test board. La prova privata e' stata fermata, senza
toccare il servizio attivo. Le prove mirate successive non sospendono i test.

## Criteri di chiusura

- Verificare separatamente bootstrap, messaggi, rifiuti e rilascio degli handle
  entro la scadenza funzionale originale, usando un marcatore nativo esplicito.
- Se il ritardo riguarda il post-main dei sanitizer, separarne l'attesa
  limitata dalla scadenza funzionale; attendere comunque il processo e
  richiedere uscita zero, senza disattivare ASan/UBSan o leak detection.
- Non sospendere prove con deadline per raffreddare: ottenere margine prima
  della prova o interromperla come misura non valida. Soglie invariate.
- Riprova ARM dei casi positivi/negativi e della suite interessata, senza
  errori mascherati o qualificazione sintetica presentata come prova camera.

La suite locale, le prove ARM e il test integrato di un hotfix restano evidenze
distinte. La chiusura sopra deriva dalla riprova specifica ARM corretta, non
dal solo esito precedente locale o di un altro provider.
