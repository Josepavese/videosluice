# Porta analysis packet dichiarata ma non alimentata nel percorso FAP causale

Stato: aperta. Backlog contratti diagnostici/API video-broker; non completata
trasferendola fuori dalla qualifica recovery/continuità notturna test board.

La r19 del 28 settembre ha superato il postflight senza turnover del Tracker
e con archive effettivamente in scrittura. Il client diagnostico si è connesso
alla porta analysis dichiarata nel catalogo e ha verificato SO_PEERCRED del
broker supervisionato; la prima lettura dell'header NV12 è scaduta dopo 2 s.
Nessun payload ricevuto: la pressione di un reader che smette di leggere non
è stata applicata, non qualificata e non diventa PASS.

Evidenze originali nel PAL root-private
`/var/lib/nemesi/state/operator-backups/capture-night-20260928-r19/`:
`initial-clean-baseline-pass.json`, `fault-baseline.json`,
`diagnostic-catalog.json`, `chain-failure.json`, sorgente del controller.
La candidata installata resta manifest fce129bd…ef13849; nessun deploy dovuto
al test. La continuazione nativa riusa solo baseline ancora applicabile.

Nel V4L2 bridge `frame_source.enabled` pubblica il frame analysis nella
publication RAM; il `write_mux_frame` packet è nell'else. L'observer causale
Go aggiorna i contatori senza mantenere i pixel. Il listener packet continua
invece ad accettare client: connettività e contatori analysis non provano che
questo endpoint consegni immagini. Il Tracker reale usa il delivery SDK.

Intervento futuro: rendere capabilities/admission/diagnostica coerenti con
l'effettivo piano dei frame. Una porta non supportata deve essere indisponibile
o rifiutare esplicitamente, non accettare silenziosamente. Se serve una porta
diagnostica nel percorso causale, usare il medesimo contratto autenticato e
buffer/budget limitati, senza ripristinare trasporti legacy o raddoppiare ogni
frame per un consumer assente. Correggere catalogo, documentazione e test
correlati insieme; provare consumer lento/disconnesso su quel percorso reale.

La README non dichiara più questa porta operativa sotto FAP causale. Questo
chiarimento non è la correzione runtime e non chiude l'issue. Non sospendere
il Tracker/BPU per compensare la mancanza di un probe sicuro.
