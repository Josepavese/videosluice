# Modalita' sicure delle sorgenti e risoluzione per ogni output

## Stato e obiettivo

Proposta di miglioramento, 2026-09-05. Implementazione completata e verificata
localmente il 2026-09-06; qualificazione hardware e rilascio ancora aperti.

### Avanzamento 2026-09-06

Implementati contratto, integrazione centrale, backend e pannello:

- `source_modes` contiene alternative qualificate per una sorgente, esatte
  per formato/dimensioni/FPS. La sorgente base resta il default autorevole.
  Non si modificano identita', path, controlli camera, provider o sicurezza;
- `source_selections` persiste gli override nello stesso piano CAS insieme
  alle scelte output e al rollback. Omissione/null mantiene gli override;
  `{}` li azzera; le preferenze per sorgenti inattive restano memorizzate;
- estensione del solo contratto corrente catalog v1/plan v2/control v2,
  senza reader legacy o seconda configurazione della UI. Il policy builder
  include le modalita' nella revisione SHA256 del catalogo centrale;
- preview senza aprire camere ne' acquisire lease: verifica provider, risorse
  occupate e prenotazioni concorrenti su una copia dei contatori;
- cambio acquisizione o risoluzione strutturale con stop/join del proprietario
  interessato prima del nuovo avvio; attestazione e frame freschi prima della
  persistenza. Ogni consumer della stessa capture avanza generazione, mentre
  le altre sorgenti restano gestite dal loro proprietario corrente;
- rollback verifica nuovamente hardware e frame del percorso precedente,
  anche se il timeout del tentativo e' scaduto. Un rollback fallito resta un
  errore esplicito, non una finta riuscita;
- domanda live temporanea rilasciata dopo verifica o rollback. Il normale
  avvio del broker valuta subito la temperatura prima di lanciare il child;
- UI per N sorgenti: acquisizione distinta da risoluzione/fluidita' output,
  combinazioni complete e compatibili, vincoli espliciti, bozza/confermato,
  anteprima degli output coinvolti e avviso della breve interruzione.

Test verificati: suite broker completa anche race; modalita' condivise,
persistenza con nuova istanza Store, sorgenti inattive, revoca centrale,
conflitto FPS, provider assente, capacita' occupata, prenotazione concorrente,
camera rimossa, hardware rifiutato, timeout al primo frame, ripristino e resize
con fencing di tutti i consumer della sola camera interessata. Round-trip
Unix IPC e facciata HTTP conservano la distinzione omissione/override/reset.
Policy Python e contratti integrazione/soak aggiornati. Pannello renderizzato
con cinque sorgenti anche a 390 px; test JS verdi.

Queste sono prove locali, con starter controllato e socket/SHM reali, non
qualificazioni dei dispositivi. Nessun profilo centrale test board/deployed board e' stato
modificato: le alternative vanno aggiunte solo dopo qualifica del relativo
hardware. Restano aperti matrice board/provider, camere reali, misure di
CPU/RAM/temperature/SD e soak test board, poi eventuale rilascio deployed board.

Richiesta: nel pannello impostazioni del webserver non limitarsi al profilo
opaco "Bilanciato". Per ogni sorgente rendere consultabili e selezionabili le
modalita' di lettura sicure, con formato e risoluzione; per ogni output
consentire la scelta della risoluzione fra quelle compatibili.

La responsabilita' del catalogo e della validazione appartiene al prodotto
standalone `video-broker`; il webserver presenta le scelte all'operatore.
La richiesta iniziale era documentale; la successiva richiesta dell'utente
ha autorizzato l'implementazione dei punti 4 e 5. Questo registro non attesta
un deploy o una modifica delle configurazioni attive sui dispositivi.

## Punto di partenza del primo audit (storico)

- In `routing/contract.go`, `Catalog` contiene sorgenti e opzioni per stream;
  `StreamOption` lega una sorgente a codec/formato pixel, dimensioni, FPS e
  parametri dell'output. Il piano persistito seleziona ID di opzioni, con
  revisioni del catalogo e del piano.
- In `routing/control.go`, `ControlStreamOption` espone gia' dimensioni e
  formato dell'output; `ControlCatalog.Sources` espone solo gli ID delle
  sorgenti, non una lista pubblica di modalita' di acquisizione selezionabili.
- In `../webserver/webapp/app.js`, `videoRoutingProfileLabel` costruisce
  etichette relative come "Piu' dettaglio" e "Bilanciato".
  `renderVideoRouting` mostra "Telecamera" e "Comportamento", senza
  riportare risoluzione e formato nelle etichette delle opzioni.

"Bilanciato" e' quindi anche una scelta di presentazione: non dimostra da
solo che camera o broker supportino una sola modalita'. La presenza di una
sola scelta sul dispositivo e' una segnalazione dell'utente; questo documento
non certifica il catalogo attualmente distribuito o le capability live.

## Due livelli distinti di configurazione

### 1. Modalita' di acquisizione, per sorgente

Ogni sorgente deve esporre un insieme finito di modalita' qualificate. Una
modalita' e' una combinazione completa: formato/codec in ingresso, larghezza,
altezza e cadenza supportata, con un ID stabile. Gli FPS vanno inclusi anche
se inizialmente non sono un controllo separato: formato e risoluzione da soli
non identificano una modalita' sicura.

Non costruire il prodotto cartesiano di formati, dimensioni e FPS rilevati:
una combinazione specifica deve essere realmente supportata e qualificata.
Separare sempre formato compresso della camera, pixel format dopo il decode
e codec/pixel format dell'output.

Mostrare modalita' attiva, modalita' desiderata, disponibilita' e motivazione
di un eventuale vincolo. La semplice enumerazione del driver non basta per
qualificare una modalita' come sicura; il rilevamento non deve interferire
con una capture attiva.

### 2. Risoluzione, per output

Ogni stream deve poter scegliere sorgente e risoluzione d'uscita fra profili
ammessi. "Diretta", "Analisi" e "Registrazione" sono le etichette Nemesi:
il contratto del broker deve restare riusabile per stream con nomi arbitrari.

La scelta di risoluzione seleziona un profilo completo e validato. Codec,
pixel format, FPS e bitrate applicabili devono restare visibili nei dettagli;
non devono cambiare silenziosamente come effetto di un'etichetta ambigua.
Non e' necessario esporre ogni parametro come controllo modificabile.

Sorgente e output possono avere risoluzioni differenti solo quando il graph
dispone delle trasformazioni ammesse. Il passthrough nativo non offre un
ridimensionamento gratuito: se manca una trasformazione qualificata, la
risoluzione non e' selezionabile. Esplicitare eventuali vincoli di aspect
ratio, crop, allineamento e assenza di dettaglio aggiuntivo nell'upscaling.

### Vincolo delle sorgenti condivise

Una sorgente fisica conserva un solo proprietario della capture e una sola
modalita' di acquisizione attiva, condivisa dai suoi consumer. I profili degli
output restano indipendenti entro i vincoli del graph, non dalle risorse comuni.

Il cambio di risoluzione di un output non deve alterare gli altri se non e'
necessario. Se richiede anche un cambio della modalita' sorgente, la preview
deve dichiararlo e mostrare tutti gli output coinvolti, inclusi quelli non
modificati esplicitamente dall'operatore. Conflitti fra richieste simultanee
vanno risolti o rifiutati prima del cutover, non aprendo due volte la camera.

## Catalogo centrale: decisione raccomandata

Definire centralmente un catalogo autorevole, versionato e autenticato di
modalita' sorgente e profili output qualificati, estendendo l'autorita' gia'
esistente. Non aggiungere una seconda configurazione indipendente nel webserver.

La centralizzazione e' responsabilita' dell'integratore: Nemesi usa il proprio
registro centrale, mentre il broker standalone accetta lo stesso contratto
generico senza dipendere da servizi o logiche Nemesi. Non richiedere una
connessione permanente al registro: usare localmente il catalogo autenticato
gia' accettato, secondo la sua policy di validita'.

Le scelte applicabili sono l'intersezione fra:

- modalita' e profili autorizzati dal catalogo;
- capability effettive della sorgente e dei provider disponibili;
- compatibilita' dell'intero piano con le risorse e lo stato runtime.

Conservare separati "dichiarato", "qualificato" e "attestato in esecuzione".
Associare la qualificazione a camera, piattaforma/provider e condizioni di
concorrenza pertinenti: un limite osservato su un singolo hardware non diventa
una policy universale. Una sorgente sconosciuta non eredita automaticamente
le modalita' sicure di un'altra camera.

Verificare decoder, scaler, encoder e ogni altro acceleratore coinvolto
(JPU, VPU, GPU, BPU quando pertinente), banda USB condivisa, memoria, copie,
CPU, sessioni concorrenti e protezioni termiche. La feature deve funzionare
per tutte le piattaforme supportate, non solo RDK X5, e con N sorgenti senza
limiti artificiali a due camere. Mantenere H265-first per gli output compressi
Nemesi e rispettare i profili hardware obbligatori: nessun fallback software
implicito per rendere selezionabile una combinazione non supportata.

## Esperienza nel pannello webserver

- Sezione "Telecamere": per ogni sorgente, "Modalita' di acquisizione" con
  formato, dimensioni e FPS reali; evidenziare quella consigliata e quella
  attiva. Esempio illustrativo di etichetta: "1280 x 720 - MJPG - 20 fps";
  non e' una dichiarazione di supporto per tutte le camere.
- Sezione "Funzioni video": per ogni output, "Telecamera" e "Risoluzione".
  Etichette come "HD - 1280 x 720" mantengono leggibilita' e precisione.
  "Bilanciato" puo' restare un suggerimento, non l'unica informazione.
- Mostrare soltanto opzioni qualificate; distinguere quelle temporaneamente
  indisponibili spiegando il motivo in linguaggio semplice. Se esiste una
  sola opzione, mostrarne i dettagli e il vincolo, senza inventare alternative.
- Riepilogare prima della conferma acquisizione, output e interruzioni attese.
  Riutilizzare tema scuro, componenti e accessibilita' delle altre impostazioni.
- Non esporre path dei dispositivi, worker, parametri vendor o limiti di
  sicurezza come campi arbitrariamente editabili dall'operatore.

## Applicazione live e persistenza

Estendere il Control Plane corrente e la sequenza preview/apply/status/rollback;
non introdurre un secondo percorso di applicazione. Se occorre un cambio di
schema, adottare un solo contratto corrente e aggiornare insieme i consumer,
preservando le scelte utili tramite conversione una tantum quando possibile.

Persistire nel piano autorevole sia la scelta di acquisizione sia le scelte
degli output, con ID qualificati e controllo delle revisioni. I dettagli del
contratto corrente sono specificati nell'avanzamento e in `PROTOCOL.md`:
gli ID non devono essere decodificati
dalla UI per ricostruire parametri o path interni.

La preview non prenota risorse: l'apply deve ripetere i controlli di ammissione,
confermare il primo frame e il contratto osservato, quindi completare la
transazione o eseguire il rollback verificato. Quando una camera non permette
due capture simultanee, dichiarare l'interruzione bounded necessaria alla
riconfigurazione; non promettere uno switch senza perdita di frame.

Le scelte gia' supportate dalla release devono essere modificabili senza un
nuovo deploy; eventuali riavvii dei worker interessati sono parte del cambio
controllato. La scelta deve sopravvivere a restart e aggiornamenti, restando
subordinata al catalogo centrale. Se una revisione revoca una scelta, applicare
una riconciliazione esplicita e osservabile, non un default nascosto o il cambio
automatico di camera. Distinguere sempre configurazione desiderata da effettiva.

Switch e rollback devono conservare policy on demand e protezioni termiche:
non attivare o lasciare attivo un encoder senza consumer per mostrare le
impostazioni; non ripristinare target fuori dai limiti correnti dopo un cambio.

## Criteri di accettazione e verifiche future

1. Una sorgente con piu' modalita' qualificate mostra combinazioni esatte e
   distinguibili; una con una sola modalita' ne spiega il limite.
2. Ogni output permette una risoluzione indipendente quando il graph lo
   consente, pubblicando configurazione desiderata ed effettiva verificabile.
3. Modalita' non qualificate, conflitti sulla capture condivisa e risorse
   insufficienti sono rifiutati senza lasciare un piano applicato a meta'.
4. Testare preview stale, due operatori concorrenti, camera rimossa, provider
   assente, errore al primo frame e rollback, senza fallback silenziosi.
5. Testare N sorgenti, input compresso/raw, passthrough e trasformazioni con
   i provider supportati; non qualificare tutte le board dal solo test RDK X5.
6. Verificare persistenza dopo restart/update, revisione centrale revocata e
   conservazione delle scelte ancora valide, senza percorsi runtime legacy.
7. Misurare latenza, frame age, FPS, CPU, memoria, temperature e risorse HW
   durante cambi ripetuti e soak; verificare anche zero spettatori e limiti
   termici dopo switch. Non ridurre le soglie per far passare un hardware debole.
8. Verificare il pannello reale desktop/mobile: dimensioni e formati leggibili,
   motivazioni dei blocchi, coerenza del tema e risultato applicato osservabile.

Ordine suggerito per l'implementazione futura: contratto/catalogo e test di
ammissione, transazioni/persistenza, UI, qualificazione hardware. In Nemesi
validare prima su test board, poi su deployed board quando stabile e secondo il suo profilo
centrale; le prove e i rilasci non fanno parte di questa richiesta documentale.
