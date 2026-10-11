# Extraction and evidence

Origin: Nemesi `embedded/src/video-broker`, a standalone Go/C/Python module.
All tracked code, native workers, SDKs, fixtures, examples and diagnostic history
were migrated. Application-specific identifiers in public history were redacted;
the original documentation remains recoverable in Nemesi Git history.

Module identity changed to `github.com/Josepavese/videosluice`; transport/schema
identifiers and the Python SDK package remain protocol contracts, not executable
aliases. The installed command is `videosluice`. No runtime API changes are
required for this extraction. A native test's source path was made repository
relative instead of dependent on the former parent directory name.

The release and installer include exact commit, platform, variant and file hashes.
Hardware and Nemesi deployment evidence will be recorded after artifact
qualification; standalone CI is not live deployment proof.
