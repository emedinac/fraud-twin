# Glossary

| Term | Meaning |
| --- | --- |
| **Observable view** | Records and relationships available to an operational detector at a selected cutoff. |
| **Oracle view** | Complete latent truth and provenance retained for evaluation and audit. |
| **Point-in-time (PIT)** | A feature/label row constructed only from data available at its prediction time. |
| **Label maturity** | The time at which a label becomes available to the operational workflow. |
| **Campaign** | A coordinated fraud pattern that links actors, payments, events, or graph structure. |
| **Camouflage** | A controlled transformation that makes fraud resemble legitimate behavior or relationships. |
| **Hard negative** | A legitimate or non-target record intentionally made difficult to distinguish from fraud. |
| **Logical fingerprint** | A stable digest used to compare deterministic outputs across runs or workers. |
| **Scale checkpoint** | An atomic record of completed partitions/chunks and their fingerprints for safe resume. |
| **Run manifest** | The identity, configuration, schema, count, provenance, and output metadata for a run. |

See also: :doc:`concepts`, :doc:`configuration`, and :doc:`data-contracts`.
