import { describe, expect, it } from "vitest";

import {
    enforceGitHubCodeQLState,
    enforceGitHubCandidateSecurityState,
    enforceGitHubSecurityState,
    evaluateGitHubCodeQLState,
    evaluateGitHubCandidateSecurityState,
    evaluateGitHubSecurityState,
} from "./verify-github-security-state.mjs";

const expectedSha = "a".repeat(40);

function cleanState() {
    return {
        expectedSha,
        ref: "refs/heads/main",
        resolvedRefSha: expectedSha,
        analyses: [
            { commit_sha: expectedSha, category: "/language:actions", error: "" },
            { commit_sha: expectedSha, category: "/language:javascript-typescript", error: "" },
        ],
        codeScanningAlerts: [],
        dependabotAlerts: [],
        secretScanningAlerts: [],
    };
}

describe("GitHub zero-open-alert release gate mutation sensitivity", () => {
    it("accepts a fully analysed CodeQL-only state and rejects an alert", () => {
        const clean = cleanState();
        expect(evaluateGitHubCodeQLState(clean)).toEqual({
            ok: true,
            errors: [],
            counts: { codeScanning: 0 },
        });
        expect(enforceGitHubCodeQLState(clean)).toEqual({
            ok: true,
            errors: [],
            counts: { codeScanning: 0 },
        });

        const alerted = cleanState();
        alerted.codeScanningAlerts.push({ number: 1 } as never);
        expect(evaluateGitHubCodeQLState(alerted).ok).toBe(false);
        expect(() => enforceGitHubCodeQLState(alerted)).toThrow("CodeQL has 1 open alert(s)");
    });

    it("accepts the one fully analysed state with no open alerts", () => {
        const state = cleanState();
        expect(evaluateGitHubSecurityState(state)).toEqual({
            ok: true,
            errors: [],
            counts: { codeScanning: 0, dependabot: 0, secretScanning: 0 },
        });
        expect(() => enforceGitHubSecurityState(state)).not.toThrow();
    });

    it.each([
        {
            mutation: "an open CodeQL alert",
            mutate: (state: ReturnType<typeof cleanState>) => state.codeScanningAlerts.push({ number: 11 } as never),
            error: "CodeQL has 1 open alert(s)",
        },
        {
            mutation: "an open Dependabot alert",
            mutate: (state: ReturnType<typeof cleanState>) => state.dependabotAlerts.push({ number: 12 } as never),
            error: "Dependabot has 1 open alert(s)",
        },
        {
            mutation: "an open secret-scanning alert",
            mutate: (state: ReturnType<typeof cleanState>) => state.secretScanningAlerts.push({ number: 13 } as never),
            error: "secret scanning has 1 open alert(s)",
        },
        {
            mutation: "a release commit different from main",
            mutate: (state: ReturnType<typeof cleanState>) => {
                state.resolvedRefSha = "b".repeat(40);
            },
            error: `does not match refs/heads/main at ${"b".repeat(40)}`,
        },
        {
            mutation: "a missing Actions analysis",
            mutate: (state: ReturnType<typeof cleanState>) => {
                state.analyses = state.analyses.filter((analysis) => analysis.category !== "/language:actions");
            },
            error: "lacks a successful CodeQL analysis for /language:actions",
        },
        {
            mutation: "a failed JavaScript and TypeScript analysis",
            mutate: (state: ReturnType<typeof cleanState>) => {
                state.analyses[1].error = "analysis failed";
            },
            error: "lacks a successful CodeQL analysis for /language:javascript-typescript",
        },
        {
            mutation: "analyses from an older commit",
            mutate: (state: ReturnType<typeof cleanState>) => {
                state.analyses = state.analyses.map((analysis) => ({ ...analysis, commit_sha: "c".repeat(40) }));
            },
            error: "lacks a successful CodeQL analysis",
        },
        {
            mutation: "a malformed alert response",
            mutate: (state: ReturnType<typeof cleanState>) => {
                state.codeScanningAlerts = undefined as never;
            },
            error: "response was missing or malformed",
            expectedCount: 0,
        },
        {
            mutation: "a malformed CodeQL analysis entry",
            mutate: (state: ReturnType<typeof cleanState>) => {
                state.analyses = [undefined as never];
            },
            error: "lacks a successful CodeQL analysis",
        },
    ])("turns the gate red for $mutation", ({ mutate, error, expectedCount }) => {
        const state = cleanState();
        mutate(state);

        const result = evaluateGitHubSecurityState(state);

        expect(result.ok).toBe(false);
        expect(result.errors.join("\n")).toContain(error);
        if (expectedCount !== undefined) expect(result.counts.codeScanning).toBe(expectedCount);
        expect(() => enforceGitHubSecurityState(state)).toThrow(error);
    });
});

function cleanCandidate() {
    return {
        ...cleanState(),
        ref: "refs/pull/7/merge",
        candidateAudit: { sourceSha: expectedSha, exitCode: 0, vulnerabilities: 0 },
    };
}

describe("GitHub candidate security gate mutation sensitivity", () => {
    it("accepts a merge ref with a multi-digit pull-request number", () => {
        const state = cleanCandidate();
        state.ref = "refs/pull/73/merge";
        expect(evaluateGitHubCandidateSecurityState(state).ok).toBe(true);
    });
    it("accepts an audited candidate while the release gate retains default-branch alerts", () => {
        const state = cleanCandidate();
        state.dependabotAlerts.push({ number: 17 } as never);
        expect(evaluateGitHubCandidateSecurityState(state)).toEqual({
            ok: true,
            errors: [],
            counts: { codeScanning: 0, secretScanning: 0 },
        });
        expect(enforceGitHubCandidateSecurityState(state).ok).toBe(true);
        expect(evaluateGitHubSecurityState(state).ok).toBe(false);
    });

    it.each([
        [
            "missing audit",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.candidateAudit = undefined as never;
            },
        ],
        [
            "wrong audited commit",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.candidateAudit.sourceSha = "b".repeat(40);
            },
        ],
        [
            "failed audit process",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.candidateAudit.exitCode = 1;
            },
        ],
        [
            "audit findings",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.candidateAudit.vulnerabilities = 1;
            },
        ],
        [
            "malformed audit findings",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.candidateAudit.vulnerabilities = "0" as never;
            },
        ],
        [
            "main ref",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = "refs/heads/main";
            },
        ],
        [
            "missing ref",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = undefined as never;
            },
        ],
        [
            "zero pull request",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = "refs/pull/0/merge";
            },
        ],
        [
            "non-numeric pull request",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = "refs/pull/7x/merge";
            },
        ],
        [
            "leading zero pull request",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = "refs/pull/07/merge";
            },
        ],
        [
            "prefixed merge ref",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = "prefix/refs/pull/7/merge";
            },
        ],
        [
            "suffixed merge ref",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = "refs/pull/7/merge/suffix";
            },
        ],
        [
            "object merge ref",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.ref = { toString: () => "refs/pull/7/merge" } as never;
            },
        ],
        [
            "changed merge commit",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.resolvedRefSha = "b".repeat(40);
            },
        ],
        [
            "CodeQL findings",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.codeScanningAlerts.push({ number: 18 } as never);
            },
        ],
        [
            "missing CodeQL analysis",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.analyses = [];
            },
        ],
        [
            "secret findings",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.secretScanningAlerts.push({ number: 19 } as never);
            },
        ],
        [
            "missing secret response",
            (s: ReturnType<typeof cleanCandidate>) => {
                s.secretScanningAlerts = undefined as never;
            },
        ],
    ])("rejects %s", (_label, mutate) => {
        const state = cleanCandidate();
        mutate(state);
        expect(evaluateGitHubCandidateSecurityState(state).ok).toBe(false);
        expect(() => enforceGitHubCandidateSecurityState(state)).toThrow("GitHub candidate security policy failed");
    });
});
