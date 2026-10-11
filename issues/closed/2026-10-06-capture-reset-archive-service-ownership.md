# Reset capture: attestazioni nell'albero runtime del servizio

Stato: **CORRETTO / CHIUSO per il solo tool amministrativo di recovery**.
La qualifica video/FAP, startup nativo e il goal generale restano aperti.

## Finding dimostrato

test board6ottobre: dopo power-cycle fisico confermato dall'utente, il controller
di recovery r1 passa lock/misura/quiescence ma il tool rifiuta
`camera/front/camera/frame-cleanup-recoveries`: owner999/group989/mode750,
mentre l'archivio delle attestazioni richiede UID0. Guard originale209byte
SHA `afd16cc44aa4025629b93def326237dfa7ecbf667775c49fe8acf0241363c299`
invariato; nessuna ricevuta nuova o rinomina eseguite dal tentativo FAILED.
L'attore che aveva cambiato ownership non è provato; non attribuirlo alla SD.

Il contratto precedente collocava l'archivio sotto lo stato della cattura,
legittimamente posseduto dal servizio: confondeva ownership runtime e prove
amministrative. Correggere semplicemente chown o ammettere UID999 per le
attestazioni non risolve questa separazione.

## Correzione generica, senza reader legacy

`tools/recover_capture_guard.py` richiede ora `recovery_root`/CLI
`--recovery-root`: namespace assoluto indipendente, fuori dall'albero del
guard, con directory e antenati protetti/root-owned. Vecchio percorso non
usato come default o fallback. Restano reset fisico confermato, nuovo boot,
CAS digest/incidente, doppia quiescence e receipt/fsync/idempotenza.
Gli archivi storici sono preservati, non cancellati o migrati implicitamente.
Nessuna dipendenza da Nemesi, camera specifica o logica d'identità.

10test host PASS0,023s: archivio indipendente, conservazione storico,
namespace vietati/relativi, rifiuto owner runtime/writable/symlink anche nel
validatore reale, reset/hash/stale/owner vivo/crash/CAS/idempotenza.
SHA tool `d590f1ffd1aed5a2f9fbe523a42680e24631344690f642b77e74a85a32240f9d`.

## Verifica amministrativa test board

Controller r2 sotto lock update canonico; root firmata e quattro config
verificate, proprietari capture assenti, vecchio boot
`c313a327-169e-498b-925b-d0fbdfc61ac8`, reset boot
`861712c9-2b31-4945-8b01-bd5610874ce2`. Recovery **PASS10:15:19CEST**:
guard archiviato con receipt nel PAL root-private
`/var/lib/nemesi/state/operator-backups/capture-power-recovery-20261006-r2/reset-archive/`.
Tool corretto copiato e hash-verificato come utility amministrativa privata;
nessuna modifica nella root misurata, build o nuovo deploy.
I vecchi archivi e il FAILED r1 restano conservati. Non è una prova SDK/video.

Prove private nel pack `broker-parity-20261005.drGkFjGa`:
`reset-admin-archive-regression-20261006-r1/r2.log`,
`test-board-reset-archive-metadata-20261006-r1.log`,
`test-board-power-recovery-progress-20261006-r2/r3.log` e controller r1/r2.
Il primo staging r1 non ha creato il PAL: aveva erroneamente richiesto700
al parent operator-backups root-owned755; causa corretta prima dello staging,
senza alterare permessi, policy runtime o qualifica. PAL nuovi sempre700.
