# Buffer immutabile per evitare ricalcoli SHA-256 dei frame

## Stato e vincolo prioritario

Proposta del 2026-09-09, **backlog condizionato; non implementata ne' qualificata**.
Richiesta dell'utente: valutarla **SOLO se l'impatto sul codice e' minimo e non
comporta ristrutturazioni**. Non e' un requisito aggiuntivo del goal o della
release in corso e non deve ritardare i test integrati su test board.

## Possibile miglioramento

Nel [Frame Access Plane](2026-09-01-frame-access-plane.md), valutare se un buffer
realmente immutabile, condiviso attraverso riferimenti autenticati, possa
evitare ricalcoli SHA-256 integrali ridondanti nei trasferimenti locali.
L'obiettivo e' ridurre lavoro per frame e latenza, mantenendo le garanzie
necessarie; non promettere costo zero o maggiore stabilita' senza misure.

L'immutabilita' deve essere imposta dal sistema, non soltanto dichiarata da un
puntatore read-only. Occorre verificare anche eventuali mapping scrivibili e
accessi DMA degli acceleratori VPU/JPU/GPU o di altri provider coinvolti.
La correlazione esatta cattura/stream/crop e il fencing dopo switch e restart
restano obbligatori: un buffer immutabile non li sostituisce.

## Filtro di ammissibilita': prima di sviluppare

- Fare soltanto un audit breve e circoscritto dei ricalcoli e delle garanzie
  che ciascuno protegge; identificare i punti di codice e il delta necessario.
- Procedere con una proposta implementativa solo se bastano modifiche locali
  alle primitive esistenti, senza ridisegnare trasporto, contratti pubblici,
  gestione della memoria o integrazione dei consumer.
- Se servono un nuovo sottosistema, un refactor trasversale, nuove dipendenze
  rilevanti o una ristrutturazione dei provider/SDK, **fermarsi e scartare la
  proposta per incompatibilita' con questo vincolo**, documentando il motivo.
  Non trasformarla automaticamente in una nuova roadmap.
- Non eliminare un controllo necessario: immutabilita', autenticazione e
  checksum coprono rischi diversi. Se l'equivalenza richiesta non e'
  dimostrabile nel percorso considerato, mantenere il controllo corrente.

## Audit circoscritto, 10 settembre 2026

- La mailbox nativa e' gia' RAM bounded a due slot, con seriale che evita di
  ricopiare e verificare uno slot invariato. Il writer conserva un mapping RW
  e riusa gli slot: FUTURE_WRITE non rende immutabile il frame corrente.
- Snapshot privato e integrita' tra processi restano necessari. Lo store
  accetta gia' la superficie verificata senza un ulteriore SHA completo;
  non conteggiare questa ottimizzazione esistente come lavoro nuovo.
- Candidato locale: ingress verifica l'output, DeliveryPublication lo copia
  nel buffer privato di pubblicazione, il provider atomic_publish ricalcola
  l'integrita'. Il port Allocation.Publish(ctx, []byte) accetta pero' byte
  presi in prestito, non una ownership immutabile attestata. Un flag skip_hash
  o un header con SHA valorizzato non dimostrano equivalenza delle garanzie.
- Nessuna rimozione SHA o nuova primitiva immutabile implementata. La fase di
  publication lenta osservata su test board comprende anche l'observer runtime:
  i2721ms non dimostrano costo SHA. Diagnostica separata e controllo deadline
  dopo l'attesa del lock implementati e successivamente installati su test board.

Aggiornamento16:55UTC: la qualifica startup e' distinta dalla proposta SHA;
brokerdfae8dc6 conserva l'integrita' corrente. Profilo CPU49Hz/30s, zero lost
samples, mostra fra gli hotspot publish_parts nativo e load_words del broker;
non attribuisce gli stalli al solo hash, ne' prova un leak. I13fps del Tracker
comprendono invece un difetto separato di budgeting della lettura, corretto
senza cambiare checksum o polling. Confronto integrato in corso (checklist).

La proposta rimane condizionata: prima misurare il tratto effettivamente lento
e dimostrare un delta locale sicuro. Non aprire una ristrutturazione trasversale
o dichiarare un risparmio CPU; non bloccare la qualifica test board su questa proposta.

## Verifiche se ammessa

- Nessuna regressione di integrita', provenienza, isolamento tra sorgenti,
  lifetime, quote RAM/FD, deadline e rilascio HW, anche con consumer lenti,
  cancellazione, switch e restart.
- Confronto a parita' di sorgente, profilo e carico: costo totale per frame,
  CPU, latenza e memoria, includendo copie, mapping e sincronizzazione. Il
  risparmio di hashing non deve essere annullato da nuovi costi o stalli.
- Test mirati sul percorso modificato e verifica integrata Nemesi su test board;
  nessuna qualifica universale dedotta da una sola board o da un benchmark.

Se ammessa e completata, oppure scartata, registrare esito e motivazione e
spostare questo documento in `improvements/closed/`. La sola registrazione
non autorizza implementazione, deploy o modifica dei goal.
