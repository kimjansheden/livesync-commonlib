import { describe, expect, it } from "vitest";

import { isCloudantURI } from "./utils_couchdb.ts";

describe("isCloudantURI", () => {
    it("classifies only exact IBM Cloudant hostname suffixes", () => {
        expect(isCloudantURI("https://account.cloudant.com/database")).toBe(true);
        expect(isCloudantURI("https://account.cloudantnosqldb.appdomain.cloud")).toBe(true);
        expect(isCloudantURI("https://account.cloudant.com.attacker.example")).toBe(false);
        expect(isCloudantURI("https://attacker.example/.cloudant.com")).toBe(false);
    });
});
