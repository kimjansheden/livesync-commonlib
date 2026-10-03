import { spawnSync } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

/**
 * @typedef {{
 *   bomFormat: "CycloneDX",
 *   specVersion: string,
 *   serialNumber?: string,
 *   metadata: { timestamp?: string, component: Record<string, unknown> },
 *   components: unknown[],
 *   [key: string]: unknown
 * }} CycloneDxSbom
 */

/**
 * Remove the fields which differ between two runs over the same lockfile, so the SBOM is reproducible.
 *
 * @param {unknown} source
 * @returns {CycloneDxSbom}
 */
export function normaliseCycloneDxSbom(source) {
    const candidate = /** @type {Partial<CycloneDxSbom>} */ (source);
    if (
        candidate?.bomFormat !== "CycloneDX" ||
        !Array.isArray(candidate.components) ||
        !candidate.metadata?.component
    ) {
        throw new Error("npm returned malformed CycloneDX output.");
    }

    const sbom = structuredClone(/** @type {CycloneDxSbom} */ (candidate));
    delete sbom.serialNumber;
    delete sbom.metadata.timestamp;
    return sbom;
}

export async function generateSbom(outputPath, environment = process.env) {
    const npmExecPath = environment.npm_execpath;
    if (!npmExecPath) {
        throw new Error("npm_execpath is required; run this generator through npm run sbom.");
    }

    const result = spawnSync(
        process.execPath,
        [npmExecPath, "sbom", "--sbom-format", "cyclonedx", "--package-lock-only", "--sbom-type", "library"],
        { encoding: "utf8", maxBuffer: 32 * 1024 * 1024 }
    );
    if (result.status !== 0) {
        throw new Error(`npm sbom failed: ${(result.stderr || "unknown error").trim()}`);
    }

    const sbom = normaliseCycloneDxSbom(JSON.parse(result.stdout));
    await writeFile(outputPath, `${JSON.stringify(sbom, null, 2)}\n`, "utf8");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    const outputPath = process.argv[2];
    if (!outputPath || process.argv.length !== 3) {
        console.error("Usage: npm run sbom -- <output-file>");
        process.exitCode = 1;
    } else {
        generateSbom(outputPath).catch((error) => {
            console.error(error instanceof Error ? error.message : error);
            process.exitCode = 1;
        });
    }
}
