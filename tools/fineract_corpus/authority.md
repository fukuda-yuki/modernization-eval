# Common baseline authority and provenance

Apache Fineract 1.15.0 is fixed at `d5636847ac556c30b437254c353f05526d172b97`.
Original source, public tests, documents and build definitions retain their paths under `source/`.
`reconstructed/` contains researcher-created retrospective baseline explanations dated 2026-10-16,
not original historical overview/design/PT/ST artifacts or executed test reports.
Source licenses/notices are in `source/LICENSE_SOURCE`, `source/NOTICE_SOURCE` and file-specific headers.
Extracts preserve original bytes; added headings/references and researcher prose are marked additions.
No source file is changed by this corpus producer. Model token counts are not inferred from file sizes.

| Evidence | Authority and limits |
| --- | --- |
| Final public change request | Defines the requested new behavior; overrides baseline only where explicitly stated. |
| Pinned implementation and public tests | Evidence of baseline behavior and intended checks, not proof of executed correctness. |
| architecture/business-date.adoc | Its opening says logical business date/COB do not exist. This is historical design context, not current inventory: BusinessDateApiResource, business-date services, COB modules and BusinessDate.feature exist at the same SHA. |
| features/pause-delinquency.adoc | Inclusive resume descriptions conflict with a preceding-day example; baseline helper uses resume start as end and the public integration status test expects active on resume day. Apply the final request's explicit new rule when it differs. |
| testing/index.adoc | Contains TBD; complete original per-feature PT/ST specifications have not been established. |
| Version/creation years | Presence at this SHA is established. Example dates, copyright years and later-built version websites are not original document creation dates. |

These qualifications and all reconstructed materials are common to every condition.
The organization index contains headings and references only; it is not an additional specification.
