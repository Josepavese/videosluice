# Arena NV12: frammentazione e ammissione full-frame

Stato: **BACKLOG APERTO B17**, ottimizzazione eventuale, non guasto SD provato
né garanzia di prenotazione aggiunta al contratto FAP corrente.

Sul test board firmato57f3e5e100ec, qualifica r9: un risultato full3110400byte
trattenuto; un secondo request ha risposto `CapacityRejected` entro il
budget100ms, nonostante `MaxArtifactBytes=6220800`/`MaxResults=16`.
Il test aveva confuso i tetti configurati con capacità sempre prenotata.
Non sono disponibili statistiche interne per attribuire **quel rifiuto
fisico** alla frammentazione, al working set o alla contesa: causalità aperta.
La sola latenza SD non spiega un rifiuto dell'arena in RAM.

Riproduzione host mantenuta in
[arena_fragmentation_test.go](../frameaccess/arena_fragmentation_test.go):
16frame96byte, arena18*n, tetto artifact2*n. Un ROI6byte seguito da un full
trattenuto lascia, dopo il close del ROI,6+90byte liberi non contigui.
Il secondo full96byte viene rifiutato, senza crescita di used/artifact/result;
il producer pubblica il frame17 e, dopo il close del full, una nuova
estrazione riesce. Race/count3 PASS: meccanismo **software/sintetico**, non
misura del heap reale test board o dimostrazione di leak.

La qualifica corrente deve dimostrare comportamento contrattuale:
pressione reale con almeno un risultato aperto e rifiuto esplicito/bounded;
camera **e superfici FAP native** ancora in avanzamento; risorse non crescenti,
rilascio cooperativo e stale ref rifiutate. Nessun requisito corrente promette
ammissione di ogni richiesta fino al tetto; non ridurre quote, FPS o deadline.

Evoluzione possibile dopo misure del heap reale: separare riserve frame e
artifact oppure evictare solo input vecchi non richiesti per ottenere spazio
contiguo. Preservare input richiesto/pinned, artifact immutabili, origine esatta,
quote globali e lifetime FD. Non compattare slice esposte o introdurre nearest
frame. Serve regressione causale, equivalente qualità e misura CPU/retention/
disponibilità prima di qualsiasi nuovo runtime/deploy.
Coordinare con [buffer immutabili/SHA](2026-09-09-immutable-frame-buffer-sha-cost.md),
senza portare queste evoluzioni laterali dentro la chiusura corrente.

Prove private nel pack `broker-parity-20261005.drGkFjGa` sotto
`<private-workspace>`: `test-board-goal-hardware-progress-20261006-r9-b.log`,
`test-board-goal-arena-fragmentation-reproduction-20261006-r1.log`.
R9 FAILED originale e ripristino PASS16:55:07CEST conservati; nessun hotfix
del runtime o corpus, nessun intervento deployed board.
