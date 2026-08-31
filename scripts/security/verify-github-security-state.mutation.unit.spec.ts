import { describe, expect, it } from "vitest";

import {
    enforceGitHubCodeQLState,
    enforceGitHubSecurityState,
    evaluateGitHubCodeQLState,
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
