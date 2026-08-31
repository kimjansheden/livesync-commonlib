import { describe, expect, it } from "vitest";

import {
    enforcePublishedZeroAlertStatus,
    evaluatePublishedZeroAlertStatus,
} from "./verify-published-zero-alert-status.mjs";

const now = Date.parse("2026-08-31T12:00:00.000Z");

function validStatus() {
    return {
        context: "Zero open security alerts",
        state: "success",
        description: "CodeQL=0 Dependabot=0 secret-scanning=0",
        creator: { login: "owner" },
        updated_at: "2026-08-31T11:55:00.000Z",
    };
}

function evaluate(statuses: unknown) {
    return evaluatePublishedZeroAlertStatus({
        statuses,
        expectedActor: "owner",
        now,
        maxAgeMilliseconds: 60 * 60 * 1000,
    });
}

describe("published zero-alert status mutation sensitivity", () => {
    it("accepts only a fresh, complete, owner-published success receipt", () => {
        const input = {
            statuses: [validStatus()],
            expectedActor: "owner",
            now,
            maxAgeMilliseconds: 60 * 60 * 1000,
        };
        expect(evaluatePublishedZeroAlertStatus(input)).toEqual({ ok: true, errors: [] });
        expect(() => enforcePublishedZeroAlertStatus(input)).not.toThrow();
    });

    it("throws when enforcement receives a non-success receipt", () => {
        const input = {
            statuses: [{ ...validStatus(), state: "failure" }],
            expectedActor: "owner",
            now,
            maxAgeMilliseconds: 60 * 60 * 1000,
        };
        expect(() => enforcePublishedZeroAlertStatus(input)).toThrow("is failure, not success");
    });

    it.each([
        ["malformed response", undefined, "response was malformed"],
        ["missing receipt", [], "has no Zero open security alerts attestation"],
        ["malformed entry", [undefined], "has no Zero open security alerts attestation"],
        ["red receipt", [{ ...validStatus(), state: "failure" }], "is failure, not success"],
        ["pending receipt", [{ ...validStatus(), state: "pending" }], "is pending, not success"],
        ["incomplete receipt", [{ ...validStatus(), description: "CodeQL=0" }], "complete zero-alert matrix"],
        ["wrong actor", [{ ...validStatus(), creator: { login: "other" } }], "declared repository owner"],
        ["missing actor", [{ ...validStatus(), creator: undefined }], "declared repository owner"],
        ["old receipt", [{ ...validStatus(), updated_at: "2026-08-31T10:00:00.000Z" }], "older than"],
        ["future receipt", [{ ...validStatus(), updated_at: "2026-08-31T12:01:00.000Z" }], "future-dated"],
        ["invalid date", [{ ...validStatus(), updated_at: "invalid" }], "missing, future-dated"],
    ])("fails closed for %s", (_label, statuses, expectedError) => {
        const result = evaluate(statuses);
        expect(result.ok).toBe(false);
        expect(result.errors.join("\n")).toContain(expectedError);
    });

    it("uses only the newest matching receipt", () => {
        const result = evaluate([
            { ...validStatus(), state: "failure", updated_at: "2026-08-31T11:59:00.000Z" },
            validStatus(),
            { ...validStatus(), context: "unrelated", updated_at: "2026-08-31T12:00:00.000Z" },
        ]);
        expect(result.ok).toBe(false);
        expect(result.errors.join("\n")).toContain("is failure, not success");
    });

    it("accepts a receipt stamped at the current instant", () => {
        expect(evaluate([{ ...validStatus(), updated_at: "2026-08-31T12:00:00.000Z" }]).ok).toBe(true);
    });

    it("accepts a receipt exactly at the maximum age", () => {
        expect(evaluate([{ ...validStatus(), updated_at: "2026-08-31T11:00:00.000Z" }]).ok).toBe(true);
    });

    it("ignores a newer status from another check context", () => {
        expect(
            evaluate([
                {
                    ...validStatus(),
                    context: "another required check",
                    state: "failure",
                    updated_at: "2026-08-31T11:59:00.000Z",
                },
                validStatus(),
            ]).ok
        ).toBe(true);
    });
});
