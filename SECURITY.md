# Security policy

This repository is a public security fork of `vrtmrz/livesync-commonlib`. It exists to provide reviewed, immutable Commonlib source and package artefacts for the public `kimjansheden/obsidian-livesync` security fork.

## Supported releases

Only commits and tags named by a published release receipt are supported. Consumers must pin the exact source commit and verify the packaged tarball SHA-256; a branch name or an unqualified version string is not a trusted identity.

## Reporting a vulnerability

Do not open a public issue containing an unpatched exploit, credential, private Vault path, or user data. Use this repository's private GitHub security-advisory channel. The fix will be coordinated with upstream before exploit details are published.

Security reports must use synthetic reproduction data. Never attach a real Vault, settings file, Setup URI, support export, access key, secret key, endpoint containing private identifiers, or configuration passphrase.

## Release policy

A release candidate is blocked unless the exact commit has all of the following:

- zero open CodeQL, Dependabot, and secret-scanning alerts;
- a successful JavaScript/TypeScript and GitHub Actions CodeQL analysis;
- `npm audit --audit-level=low` with zero findings;
- the complete package, boundary, unit, and managed integration gates;
- 100% mutation score for the security-critical settings-persistence and zero-alert decision logic;
- a clean tracked-file and Git-history secret scan; and
- a packed-package hash and source-commit receipt.

The statement "zero findings" means zero open verified findings in this declared matrix for one exact commit and lockfile. It is not a claim that unknown vulnerabilities cannot exist.

## Scope and trust boundary

The threat model is documented in [docs/security-threat-model.md](docs/security-threat-model.md). This fork contains only upstream-derived source, generic security changes, public tests, public documentation, and synthetic fixtures. It must not receive files or identifiers from a private consumer repository.
