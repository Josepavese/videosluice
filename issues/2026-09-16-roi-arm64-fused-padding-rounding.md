# ROI: arrotondamento padding diverso tra ARM64 e SDK Python

Stato: **APERTO — RED/GREEN verificati su test board, correttiva installata;
qualifica integrata bloccata dallo startup nativo**.
Collegata: [startup e ROI del16settembre](2026-09-16-native-startup-quarantine-and-stop-signals.md).

## Causa riproducibile

`frameaccess.MapRegion` calcolava padding e limiti con espressioni Go che il
compilatore ARM64 fonde in FMSUBD/FMADDD. Il consumer Python effettua invece
moltiplicazione e addizione/sottrazione distinte. Un ULP in prossimita' di un
bordo NV12 pari puo' produrre **2pixel di differenza**, pur con identici input,
trasformazione e prova di copia. La validazione SDK correttamente rifiuta il ROI.

Esempio riprodotto con esecuzione ARM64 su test board, senza camera/corpus:
bbox analysis`(8,10,83,110)`, padding0,08, full1920x1080/analysis640x360.
Vecchio Go`(4,4,268,354)`, Python`(6,6,268,354)`.
Anche scale1/2, mirror e rotazione90 divergono. Test
`TestMapRegionBinary64PaddingBoundary`: **RED5/7casi** su test board con codice
precedente. Non e' ancora prova che questo fosse il campo diverso nei due
errori reali del16settembre: i vecchi log aggregavano geometry/provider/evidence.

## Correttiva minima

Conversione esplicita a`float64` dei due prodotti padding in`MapRegion` impone
arrotondamento binary64 prima del calcolo bordi, coerente con lo SDK.
Nessun epsilon, tolleranza geometrica, cambiamento wire/schema o fallback.
Il disassemblato ARM64 corretto non contiene piu'FMA in`MapRegion`.
Test geometrici/race locali PASS; golden paralleli Python44test PASS.
Conservati7casi scala/rotazione/mirror: **GREEN7/7 su test board**, binario
SHA`13cdecc71f91a2101dea0c87751197753a72b82b86685416ee22979487ba1da5`.
Installata nel broker runtime `6f925237` il16settembre. Non e' prova ottica
su persone reali.

## Consegna e limiti

Scoperto mentre era gia' in corso la promozione nativa`f2bd393d`:
non interrompere quella transazione ne' modificarne payload/manifest a meta'.
Il fix Go richiede riconciliazione successiva misurata e verifica integrata;
non dichiarare la release nativa contenente questa correzione.
Build CGO ARM64 sul dispositivo PASS19:52:53UTC, test geometry7/7 PASS e
linkage verificato, temperatura massima81,346°C, runtime invariato. Test
integrato locale C/SDK/crop/Registry con race PASS15,001s. Candidato: solo
`frameaccess/types.go` e binario video-broker
SHA`1ed2383fb4d3f525e0363c4e5724e6f7b3b5150d1697a374f0fe608f4e1e3b20`.
Base esatta`f2bd393d`, nessun hotfix nativo perso. Job persistente
`/var/lib/nemesi/state/operator-backups/roi-rounding-20260916.4FYpoU57`;
piano`87dd9fa01a452dba8942add2663548c532bca376285fdbfec554e3677deedde4`.
Evidenza privata:
`<private-workspace>`;
binario RED SHA`dbb5e029d30b62434cf85c10e93c5bbf92d0edaad1dbe8b0c6b8511be6bbe0b9`.

Promozione e postflight PASS; manifest
`6f9252378d8f9eb341ced3a6eec5006fadea129ec0ce1a3023a7e43644a3bbd3`.
Readback esatto conferma tutti13file delle due promozioni e le quattro
configurazioni preservate; Go ELF installato senza FMA in MapRegion.
Misura9360entry/553725857byte PASS. La verifica funzionale successiva fallisce:
discontinuita' UVC20:10:56UTC, nuovo worker in timeout bootstrap e quarantena.
La parita' dei file non chiude quindi il gate di stabilita': vedi issue startup.
