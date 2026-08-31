import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { describe, it } from "node:test";

const releaseWorkflow = await readFile(new URL("../.github/workflows/release.yml", import.meta.url), "utf8");
const releaseRunbook = await readFile(new URL("../docs/releasing.md", import.meta.url), "utf8");

function sourceReceiptJqContract(workflow) {
    const command = workflow.split(/\r?\n/u).find((line) => line.includes("jq -n --arg repository"));
    assert.ok(command, "source-receipt jq command is missing");

    const commandParts = command.match(/jq -n (?<arguments>.*?) '(?<program>\{.*\})' >/u);
    assert.ok(commandParts?.groups, "source-receipt jq command cannot be parsed");

    const declared = [...commandParts.groups.arguments.matchAll(/--arg\s+([A-Za-z][A-Za-z0-9]*)\s+/gu)].map(
        ([, name]) => name
    );
    const referenced = [...commandParts.groups.program.matchAll(/\$([A-Za-z][A-Za-z0-9]*)/gu)].map(([, name]) => name);
    return {
        declared: [...new Set(declared)].sort(),
        referenced: [...new Set(referenced)].sort(),
    };
}

describe("attested security release workflow", () => {
    it("runs the managed integration gate before artefact creation and publication", () => {
        const integrationGate = releaseWorkflow.indexOf("npm run test:integration:managed");
        const artefactCreation = releaseWorkflow.indexOf("name: Create package, SBOM");
        const publication = releaseWorkflow.indexOf("name: Publish immutable GitHub release");

        assert.notEqual(integrationGate, -1, "managed integration gate is missing");
        assert.ok(integrationGate < artefactCreation, "managed integration gate must precede artefact creation");
        assert.ok(integrationGate < publication, "managed integration gate must precede publication");
    });

    it("documents the current signed-tag workflow and immutable inputs", () => {
        assert.match(releaseRunbook, /gh workflow run release\.yml/u);
        assert.match(releaseRunbook, /git verify-tag "\$tag"/u);
        assert.match(releaseRunbook, /-f tag="\$tag"/u);
        assert.match(releaseRunbook, /-f expected_sha="\$release_sha"/u);
        assert.doesNotMatch(releaseRunbook, /gh workflow run publish-github-release\.yml/u);
    });

    it("declares every source-receipt jq variable under the referenced name", () => {
        const contract = sourceReceiptJqContract(releaseWorkflow);
        assert.deepEqual(contract.referenced, contract.declared);
    });

    it("rejects drift between a declared jq argument and its receipt reference", () => {
        const mutatedWorkflow = releaseWorkflow.replace(
            "lockfileSha256:$lockfileSha256",
            "lockfileSha256:$lockfileSha"
        );
        const contract = sourceReceiptJqContract(mutatedWorkflow);
        assert.notDeepEqual(contract.referenced, contract.declared);
    });
});
