---
date: 2026-08-31
commonlib-version-reviewed: "0.1.19-security.1"
self-hosted-livesync-version-reviewed: "1.0.21"
status: public security-fork release candidate
---

# Commonlib security threat model

## Assets

Commonlib processes remote credentials, configuration-encryption passphrases, Vault-encryption passphrases, remote profile URIs, file paths, note contents, conflict revisions, and synchronisation metadata. A host is responsible for acquiring these values and for its final persistence adapter, but Commonlib must not hand that adapter a plaintext credential-bearing settings copy when encrypted persistence is configured.

## Trust boundaries

- The host owns UI, user confirmation, credential acquisition, device-local storage, and the final settings write.
- Commonlib owns transformation of runtime settings into the encrypted persistence representation and restoration into runtime memory.
- Object Storage, CouchDB, and P2P transports are untrusted remote boundaries. End-to-end encryption protects Vault content; transport credentials and configuration material require separate at-rest protection.
- Package consumers trust only an exact source commit and the hash of the packed artefact produced from it.
- GitHub Actions receive no private consumer data or repository secrets. External actions are pinned to full commit SHAs.

## Principal threats and controls

| Threat                                                                             | Control                                                                                                                                                                                                                                                                                       |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A pure S3/R2 profile bypasses the legacy CouchDB-only encryption trigger           | Every CouchDB and Object Storage connection marker activates the same encrypted persistence path.                                                                                                                                                                                             |
| Auxiliary headers, JWT keys, or transport options remain beside the encrypted blob | The persisted copy clears every connection field and restores only recognised fields after decryption.                                                                                                                                                                                        |
| A future test mutation weakens the trigger, scrub, restore, or alert decision      | Stryker must kill every supported mutant; the required threshold is 100%.                                                                                                                                                                                                                     |
| A scanner reports an alert while ordinary tests remain green                       | The workflow blocks on CodeQL. An authenticated external release gate additionally queries Dependabot and secret scanning, fails closed for malformed or stale responses or any open alert, and publishes `Zero open security alerts` on the exact commit without storing repository secrets. |
| A manually dispatched inherited workflow executes an untrusted ref                 | Input-selected package-publication and downstream-execution workflows are removed from this fork. Release and consumer verification use exact, reviewed commits through the fork's own non-privileged gates.                                                                                  |
| A compromised or floating CI action changes release behaviour                      | Every external action is pinned to one full commit SHA and inherited workflows are reviewed before activation.                                                                                                                                                                                |
| A package differs from the reviewed source                                         | Package-boundary tests build and install the exact tarball; release receipts bind source commit, lockfile, tarball SHA-256, and package integrity.                                                                                                                                            |
| Private project context reaches this public fork                                   | No private-repository export path exists. Only generic source, tests, documentation, synthetic fixtures, and public hashes are allowed.                                                                                                                                                       |

## Security invariants

1. The persisted settings copy contains no plaintext connection credential or plaintext remote-profile URI.
2. Encryption and decryption cover the same complete allowlist of connection fields.
3. Unknown decrypted properties are ignored rather than assigned to host settings.
4. Missing configuration-encryption material causes the maintained LiveSync host to fail closed before writing settings.
5. Release qualification is attached to one exact source commit and lockfile, never to a moving branch.

## Verification limits

The declared matrix covers first-party TypeScript and JavaScript, GitHub Actions, the complete locked dependency graph, package boundaries, synthetic real-service integration, and the maintained LiveSync consumer. It does not prove the absence of an unknown zero-day or manually review every line of every transitive dependency. Those limits must remain explicit in each release receipt.
