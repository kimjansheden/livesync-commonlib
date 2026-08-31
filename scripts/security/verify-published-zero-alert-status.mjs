import { pathToFileURL } from "node:url";

const STATUS_CONTEXT = "Zero open security alerts";
const SUCCESS_DESCRIPTION = "CodeQL=0 Dependabot=0 secret-scanning=0";

export function evaluatePublishedZeroAlertStatus({ statuses, expectedActor, now, maxAgeMilliseconds }) {
    const errors = [];
    if (!Array.isArray(statuses)) {
        return { ok: false, errors: ["Commit status response was malformed; the release gate fails closed."] };
    }

    const status = statuses.find((candidate) => candidate?.context === STATUS_CONTEXT);
    if (!status) {
        errors.push(`The exact release commit has no ${STATUS_CONTEXT} attestation.`);
    } else {
        if (status.state !== "success")
            errors.push(`${STATUS_CONTEXT} is ${status.state ?? "malformed"}, not success.`);
        if (status.description !== SUCCESS_DESCRIPTION) {
            errors.push(`${STATUS_CONTEXT} does not carry the complete zero-alert matrix receipt.`);
        }
        if (status.creator?.login !== expectedActor) {
            errors.push(`${STATUS_CONTEXT} was not published by the declared repository owner.`);
        }
        const updatedAt = Date.parse(status.updated_at ?? "");
        const age = now - updatedAt;
        if (!Number.isFinite(updatedAt) || age < 0 || age > maxAgeMilliseconds) {
            errors.push(`${STATUS_CONTEXT} is missing, future-dated, or older than the allowed release window.`);
        }
    }

    return { ok: errors.length === 0, errors };
}

export function enforcePublishedZeroAlertStatus(input) {
    const result = evaluatePublishedZeroAlertStatus(input);
    if (!result.ok) {
        throw new Error(`Published zero-alert status policy failed:\n- ${result.errors.join("\n- ")}`);
    }
    return result;
}

// Stryker disable all: API transport and CLI plumbing are integration boundaries; mutate the policy decision above.
async function requestStatuses({ repository, expectedSha, token, apiUrl, fetchImplementation }) {
    const url = `${apiUrl.replace(/\/$/u, "")}/repos/${repository}/commits/${expectedSha}/statuses?per_page=100`;
    const response = await fetchImplementation(url, {
        headers: {
            Accept: "application/vnd.github+json",
            Authorization: `Bearer ${token}`,
            "X-GitHub-Api-Version": "2022-11-28",
        },
    });
    if (!response.ok) {
        throw new Error(`GitHub commit-status request failed closed: ${response.status} ${response.statusText}.`);
    }
    return response.json();
}

export async function runPublishedZeroAlertStatusGate(environment = process.env) {
    const repository = environment.GITHUB_REPOSITORY;
    const expectedSha = environment.EXPECTED_SHA;
    const token = environment.GH_TOKEN;
    const expectedActor = environment.EXPECTED_STATUS_ACTOR || environment.GITHUB_REPOSITORY_OWNER;
    if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/u.test(repository ?? "")) {
        throw new Error("GITHUB_REPOSITORY must identify one owner/repository pair.");
    }
    if (!/^[0-9a-f]{40}$/u.test(expectedSha ?? "")) {
        throw new Error("EXPECTED_SHA must be one full lowercase Git commit SHA.");
    }
    if (!token) throw new Error("GH_TOKEN is required to read the protected commit status.");
    if (!/^[A-Za-z0-9-]+$/u.test(expectedActor ?? "")) {
        throw new Error("EXPECTED_STATUS_ACTOR must identify the repository owner.");
    }

    const statuses = await requestStatuses({
        repository,
        expectedSha,
        token,
        apiUrl: environment.GITHUB_API_URL || "https://api.github.com",
        fetchImplementation: fetch,
    });
    enforcePublishedZeroAlertStatus({
        statuses,
        expectedActor,
        now: Date.now(),
        maxAgeMilliseconds: 60 * 60 * 1000,
    });
    console.log(`Fresh ${STATUS_CONTEXT} attestation accepted for exact commit ${expectedSha}.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    runPublishedZeroAlertStatusGate().catch((error) => {
        console.error(error instanceof Error ? error.message : error);
        process.exitCode = 1;
    });
}
