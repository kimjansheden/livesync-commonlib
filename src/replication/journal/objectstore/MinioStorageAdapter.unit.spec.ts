import type { S3 } from "@aws-sdk/client-s3";
import type { FetchHttpHandler } from "@smithy/fetch-http-handler";
import { HttpResponse } from "@smithy/protocol-http";
import { reactiveSource } from "octagonal-wheels/dataobject/reactive";
import { promiseWithResolvers } from "octagonal-wheels/promises";
import { describe, expect, it, vi } from "vitest";

import type { BucketSyncSetting } from "@lib/common/types.ts";
import type { LiveSyncJournalReplicatorEnv } from "@lib/replication/journal/LiveSyncJournalReplicatorEnv.ts";
import { MinioStorageAdapter } from "./MinioStorageAdapter.ts";

type MockS3Client = {
    listObjectsV2?: ReturnType<typeof vi.fn>;
    send: ReturnType<typeof vi.fn>;
};

function createAdapter(client: MockS3Client) {
    const requestCount = reactiveSource(0);
    const responseCount = reactiveSource(0);
    const settings = {
        endpoint: "https://example.invalid",
        accessKey: "access-key",
        secretKey: "secret-key",
        bucket: "bucket",
        region: "us-east-1",
        bucketPrefix: "test/",
        forcePathStyle: true,
        useCustomRequestHandler: false,
        bucketCustomHeaders: "",
    } as BucketSyncSetting;
    const env = {
        services: {
            API: {
                getCustomFetchHandler: () => undefined,
                requestCount,
                responseCount,
            },
        },
    } as unknown as LiveSyncJournalReplicatorEnv;
    const adapter = new MinioStorageAdapter(settings, env);
    adapter._instance = client as unknown as S3;
    return { adapter, requestCount, responseCount };
}

describe("MinioStorageAdapter physical request activity", () => {
    it("tracks an SDK command while it is in progress", async () => {
        const request = promiseWithResolvers<object>();
        const { adapter, requestCount, responseCount } = createAdapter({ send: vi.fn(() => request.promise) });

        const uploading = adapter.upload("file.txt", new TextEncoder().encode("content"), "text/plain");

        await vi.waitFor(() => expect(requestCount.value - responseCount.value).toBe(1));
        request.resolve({});
        await expect(uploading).resolves.toBe(true);
        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
    });

    it("keeps a download active until its response body has been consumed", async () => {
        const body = promiseWithResolvers<Uint8Array>();
        const send = vi.fn(() =>
            Promise.resolve({
                Body: {
                    transformToByteArray: () => body.promise,
                },
            })
        );
        const { adapter, requestCount, responseCount } = createAdapter({ send });

        const downloading = adapter.download("file.txt");

        await vi.waitFor(() => expect(requestCount.value - responseCount.value).toBe(1));
        body.resolve(new Uint8Array([1, 2, 3]));
        await expect(downloading).resolves.toEqual(new Uint8Array([1, 2, 3]));
        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
    });

    it("distinguishes a missing object from an unavailable object store", async () => {
        const missing = Object.assign(new Error("The specified key does not exist"), { name: "NoSuchKey" });
        const { adapter } = createAdapter({ send: vi.fn(() => Promise.reject(missing)) });

        await expect(adapter.downloadWithResult("missing.json")).resolves.toEqual({ status: "not-found" });
    });

    it("reports a response without a body as unavailable rather than missing", async () => {
        const { adapter } = createAdapter({ send: vi.fn(() => Promise.resolve({})) });

        // An empty answer says nothing about whether the object exists. Calling it missing would let a caller
        // treat the remote as empty and replace key material which is still in use.
        await expect(adapter.downloadWithResult("settings.json")).resolves.toMatchObject({ status: "unavailable" });
    });

    it("preserves an object-store failure in the detailed download result", async () => {
        const failure = new Error("network failed");
        const { adapter } = createAdapter({ send: vi.fn(() => Promise.reject(failure)) });

        await expect(adapter.downloadWithResult("settings.json")).resolves.toEqual({
            status: "unavailable",
            error: failure,
        });
    });

    it("returns the object ETag with a successful read", async () => {
        const { adapter } = createAdapter({
            send: vi.fn(() =>
                Promise.resolve({
                    ETag: '"milestone-v1"',
                    Body: { transformToByteArray: () => Promise.resolve(new Uint8Array([1, 2])) },
                })
            ),
        });

        await expect(adapter.downloadWithResult("milestone.json")).resolves.toEqual({
            status: "available",
            value: new Uint8Array([1, 2]),
            etag: '"milestone-v1"',
        });
    });

    it("balances activity when an SDK command rejects", async () => {
        const { adapter, requestCount, responseCount } = createAdapter({
            send: vi.fn(() => Promise.reject(new Error("network failed"))),
        });

        await expect(adapter.upload("file.txt", new Uint8Array(), "text/plain")).resolves.toBe(false);

        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
    });

    it("tracks each supported Object Storage command unit", async () => {
        const send = vi.fn(() => Promise.resolve({}));
        const listObjectsV2 = vi.fn(() => Promise.resolve({ Contents: [] }));
        const { adapter, requestCount, responseCount } = createAdapter({ listObjectsV2, send });

        await expect(adapter.listFiles("")).resolves.toEqual([]);
        await expect(adapter.deleteFiles(["file.txt"])).resolves.toBe(true);
        await expect(adapter.isAvailable()).resolves.toBe(true);
        await expect(adapter.getUsage()).resolves.toEqual({ estimatedSize: 0 });

        expect(requestCount.value).toBe(4);
        expect(responseCount.value).toBe(4);
    });

    it("follows continuation tokens until every listed journal is visible", async () => {
        const listObjectsV2 = vi
            .fn()
            .mockResolvedValueOnce({
                Contents: [{ Key: "test/first" }],
                IsTruncated: true,
                NextContinuationToken: "next-page",
            })
            .mockResolvedValueOnce({ Contents: [{ Key: "test/second" }], IsTruncated: false });
        const { adapter, requestCount, responseCount } = createAdapter({ listObjectsV2, send: vi.fn() });

        await expect(adapter.listFiles("")).resolves.toEqual(["first", "second"]);
        expect(listObjectsV2).toHaveBeenNthCalledWith(
            1,
            expect.objectContaining({ StartAfter: "test/", Prefix: "test/" }),
            expect.objectContaining({ abortSignal: expect.any(AbortSignal) })
        );
        expect(listObjectsV2).toHaveBeenNthCalledWith(
            2,
            expect.objectContaining({ ContinuationToken: "next-page", Prefix: "test/" }),
            expect.objectContaining({ abortSignal: expect.any(AbortSignal) })
        );
        expect(listObjectsV2.mock.calls[1][0]).not.toHaveProperty("StartAfter");
        expect(requestCount.value).toBe(2);
        expect(responseCount.value).toBe(2);
    });

    it("fails closed when a truncated listing cannot advance", async () => {
        const listObjectsV2 = vi.fn(() => Promise.resolve({ Contents: [{ Key: "test/first" }], IsTruncated: true }));
        const { adapter, requestCount, responseCount } = createAdapter({ listObjectsV2, send: vi.fn() });

        await expect(adapter.listFiles("")).rejects.toThrow("without a new continuation token");
        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
    });

    it("lists keys in the order in which the storage received them, whatever their names and pages", async () => {
        const listObjectsV2 = vi
            .fn()
            .mockResolvedValueOnce({
                Contents: [
                    { Key: "test/aaa", LastModified: new Date("2026-01-01T00:00:03.000Z") },
                    { Key: "test/bbb", LastModified: new Date("2026-01-01T00:00:01.000Z") },
                ],
                IsTruncated: true,
                NextContinuationToken: "next-page",
            })
            .mockResolvedValueOnce({
                Contents: [{ Key: "test/ccc", LastModified: new Date("2026-01-01T00:00:02.000Z") }],
                IsTruncated: false,
            });
        const { adapter } = createAdapter({ listObjectsV2, send: vi.fn() });

        await expect(adapter.listFilesInUploadOrder()).resolves.toEqual([["bbb"], ["ccc"], ["aaa"]]);
        expect(listObjectsV2).toHaveBeenCalledTimes(2);
    });

    it("groups keys which the storage received at the same time, because their order cannot be told", async () => {
        const storedAt = new Date("2026-01-01T00:00:01.000Z");
        const listObjectsV2 = vi.fn(() =>
            Promise.resolve({
                Contents: [
                    { Key: "test/later", LastModified: new Date("2026-01-01T00:00:02.000Z") },
                    { Key: "test/second", LastModified: storedAt },
                    { Key: "test/first", LastModified: storedAt },
                ],
            })
        );
        const { adapter } = createAdapter({ listObjectsV2, send: vi.fn() });

        await expect(adapter.listFilesInUploadOrder()).resolves.toEqual([["first", "second"], ["later"]]);
    });

    it("lists each object with its size and the time it was stored, in the same groups and order as the keys alone", async () => {
        const storedAt = new Date("2026-01-01T00:00:01.000Z");
        const later = new Date("2026-01-01T00:00:02.000Z");
        const latest = new Date("2026-01-01T00:00:03.000Z");
        const listObjectsV2 = vi.fn(() =>
            Promise.resolve({
                Contents: [
                    { Key: "test/later", LastModified: later, Size: 30 },
                    { Key: "test/second", LastModified: storedAt, Size: 20 },
                    { Key: "test/first", LastModified: storedAt, Size: 10 },
                    { Key: "test/unsized", LastModified: latest },
                ],
            })
        );
        const { adapter } = createAdapter({ listObjectsV2, send: vi.fn() });

        await expect(adapter.listObjectsInUploadOrder()).resolves.toEqual([
            [
                { key: "first", size: 10, storedAt: storedAt.getTime() },
                { key: "second", size: 20, storedAt: storedAt.getTime() },
            ],
            [{ key: "later", size: 30, storedAt: later.getTime() }],
            [{ key: "unsized", size: undefined, storedAt: latest.getTime() }],
        ]);
        await expect(adapter.listFilesInUploadOrder()).resolves.toEqual([["first", "second"], ["later"], ["unsized"]]);
        // Each listing is one request, so a caller which needs the sizes does not list twice.
        expect(listObjectsV2).toHaveBeenCalledTimes(2);
    });

    it("fails closed when a listing with sizes leaves out when an object was stored", async () => {
        const listObjectsV2 = vi.fn(() => Promise.resolve({ Contents: [{ Key: "test/untimed", Size: 10 }] }));
        const { adapter } = createAdapter({ listObjectsV2, send: vi.fn() });

        await expect(adapter.listObjectsInUploadOrder()).rejects.toThrow("without the time at which it was stored");
    });

    it("leaves an object which is already stored as it is when asked to keep it, and reports the upload as done", async () => {
        const refused = Object.assign(new Error("At least one of the pre-conditions you specified did not hold"), {
            name: "PreconditionFailed",
            $metadata: { httpStatusCode: 412 },
        });
        const send = vi.fn(() => Promise.reject(refused));
        const { adapter } = createAdapter({ send });

        await expect(
            adapter.upload("journal", new Uint8Array([1]), "application/octet-stream", { keepExisting: true })
        ).resolves.toBe(true);
        expect((send.mock.calls[0] as unknown as [{ input: { IfNoneMatch?: string } }])[0].input.IfNoneMatch).toBe("*");
    });

    it("replaces an object unless asked to keep it", async () => {
        const send = vi.fn(() => Promise.resolve({}));
        const { adapter } = createAdapter({ send });

        await expect(adapter.upload("settings.json", new Uint8Array([1]), "application/json")).resolves.toBe(true);
        expect(
            (send.mock.calls[0] as unknown as [{ input: { IfNoneMatch?: string } }])[0].input.IfNoneMatch
        ).toBeUndefined();
    });

    it("reports a refused condition as a failed upload when it did not ask to keep the object", async () => {
        const refused = Object.assign(new Error("precondition"), { name: "PreconditionFailed" });
        const { adapter } = createAdapter({ send: vi.fn(() => Promise.reject(refused)) });

        await expect(adapter.upload("settings.json", new Uint8Array([1]), "application/json")).resolves.toBe(false);
    });

    it("uses If-Match for a milestone update and refuses a stale ETag", async () => {
        const refused = Object.assign(new Error("precondition"), { name: "PreconditionFailed" });
        const send = vi.fn(() => Promise.reject(refused));
        const { adapter } = createAdapter({ send });

        await expect(
            adapter.upload("milestone.json", new Uint8Array([1]), "application/json", { ifMatch: '"milestone-v1"' })
        ).resolves.toBe(false);
        expect((send.mock.calls[0] as unknown as [{ input: { IfMatch?: string } }])[0].input.IfMatch).toBe(
            '"milestone-v1"'
        );
    });

    it("uses the strong token from a weakly wrapped ETag for a conditional update", async () => {
        const send = vi.fn(() => Promise.resolve({}));
        const { adapter } = createAdapter({ send });

        await expect(
            adapter.upload("milestone.json", new Uint8Array([1]), "application/json", {
                ifMatch: 'W/"milestone-v1"',
            })
        ).resolves.toBe(true);
        expect((send.mock.calls[0] as unknown as [{ input: { IfMatch?: string } }])[0].input.IfMatch).toBe(
            '"milestone-v1"'
        );
    });

    it("keeps a stale weakly wrapped ETag conditional and refuses the write", async () => {
        const refused = Object.assign(new Error("precondition"), {
            name: "PreconditionFailed",
            $metadata: { httpStatusCode: 412 },
        });
        const send = vi.fn(() => Promise.reject(refused));
        const { adapter } = createAdapter({ send });

        await expect(
            adapter.upload("milestone.json", new Uint8Array([1]), "application/json", {
                ifMatch: 'W/"stale-version"',
            })
        ).resolves.toBe(false);
        expect((send.mock.calls[0] as unknown as [{ input: { IfMatch?: string } }])[0].input.IfMatch).toBe(
            '"stale-version"'
        );
    });

    it("passes an unrecognised weak ETag through unchanged", async () => {
        const send = vi.fn(() => Promise.resolve({}));
        const { adapter } = createAdapter({ send });

        await expect(
            adapter.upload("milestone.json", new Uint8Array([1]), "application/json", { ifMatch: "W/unquoted" })
        ).resolves.toBe(true);
        expect((send.mock.calls[0] as unknown as [{ input: { IfMatch?: string } }])[0].input.IfMatch).toBe(
            "W/unquoted"
        );
    });

    it("fails closed when the storage does not report when an object was stored", async () => {
        const listObjectsV2 = vi.fn(() =>
            Promise.resolve({
                Contents: [
                    { Key: "test/timed", LastModified: new Date("2026-01-01T00:00:01.000Z") },
                    { Key: "test/untimed" },
                ],
            })
        );
        const { adapter } = createAdapter({ listObjectsV2, send: vi.fn() });

        await expect(adapter.listFilesInUploadOrder()).rejects.toThrow("without the time at which it was stored");
    });

    it("tracks the custom request-handler path once at the SDK command boundary", async () => {
        const request = promiseWithResolvers<{ response: HttpResponse }>();
        const handle = vi.fn(() => request.promise);
        const requestCount = reactiveSource(0);
        const responseCount = reactiveSource(0);
        const env = {
            services: {
                API: {
                    getCustomFetchHandler: () => ({ handle }) as unknown as FetchHttpHandler,
                    requestCount,
                    responseCount,
                },
            },
        } as unknown as LiveSyncJournalReplicatorEnv;
        const settings = {
            endpoint: "https://example.invalid",
            accessKey: "access-key",
            secretKey: "secret-key",
            bucket: "bucket",
            region: "us-east-1",
            bucketPrefix: "test/",
            forcePathStyle: true,
            useCustomRequestHandler: true,
            bucketCustomHeaders: "",
        } as BucketSyncSetting;
        const adapter = new MinioStorageAdapter(settings, env);

        const uploading = adapter.upload("file.txt", new TextEncoder().encode("content"), "text/plain");

        await vi.waitFor(() => {
            expect(requestCount.value - responseCount.value).toBe(1);
            expect(handle).toHaveBeenCalledOnce();
        });
        request.resolve({ response: new HttpResponse({ headers: {}, statusCode: 200 }) });
        await expect(uploading).resolves.toBe(true);
        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
    });
});

describe("MinioStorageAdapter stale request abort", () => {
    function rejectWhenAborted<T>(abortSignal: AbortSignal | undefined): Promise<T> {
        return new Promise<T>((_resolve, reject) => {
            const abort = () => reject(Object.assign(new Error("The request was aborted"), { name: "AbortError" }));
            if (abortSignal?.aborted) abort();
            abortSignal?.addEventListener("abort", abort);
        });
    }

    it("aborts an upload which started before the given time and is still in flight", async () => {
        const send = vi.fn((_command: unknown, options?: { abortSignal?: AbortSignal }) =>
            rejectWhenAborted(options?.abortSignal)
        );
        const { adapter, requestCount, responseCount } = createAdapter({ send });

        const uploading = adapter.upload("file.txt", new Uint8Array([1]), "text/plain");
        await vi.waitFor(() => expect(send).toHaveBeenCalledOnce());

        expect(adapter.abortRequestsStartedBefore(Date.now() + 1)).toBe(1);
        await expect(uploading).resolves.toBe(false);
        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
        expect(adapter.abortRequestsStartedBefore(Date.now() + 1)).toBe(0);
    });

    describe("of an upload which was given a time to finish in", () => {
        it("aborts the upload once that time has passed, and reports it as failed", async () => {
            vi.useFakeTimers();
            try {
                const send = vi.fn((_command: unknown, options?: { abortSignal?: AbortSignal }) =>
                    rejectWhenAborted(options?.abortSignal)
                );
                const { adapter, requestCount, responseCount } = createAdapter({ send });

                const uploading = adapter.upload("record", new Uint8Array([1]), "application/octet-stream", {
                    abortAfterMs: 40_000,
                });
                let settled = false;
                void uploading.then(() => (settled = true));

                await vi.advanceTimersByTimeAsync(39_999);
                expect(settled).toBe(false);
                await vi.advanceTimersByTimeAsync(1);
                await expect(uploading).resolves.toBe(false);
                expect(requestCount.value).toBe(responseCount.value);
            } finally {
                vi.useRealTimers();
            }
        });

        it("aborts at once when no time is left", async () => {
            vi.useFakeTimers();
            try {
                const send = vi.fn((_command: unknown, options?: { abortSignal?: AbortSignal }) =>
                    rejectWhenAborted(options?.abortSignal)
                );
                const { adapter } = createAdapter({ send });

                const uploading = adapter.upload("record", new Uint8Array([1]), "application/octet-stream", {
                    abortAfterMs: -5,
                });
                await vi.advanceTimersByTimeAsync(0);

                await expect(uploading).resolves.toBe(false);
            } finally {
                vi.useRealTimers();
            }
        });

        it("leaves no timer behind when the upload finishes in time", async () => {
            vi.useFakeTimers();
            try {
                const { adapter } = createAdapter({ send: vi.fn(() => Promise.resolve({})) });

                await expect(
                    adapter.upload("record", new Uint8Array([1]), "application/octet-stream", { abortAfterMs: 40_000 })
                ).resolves.toBe(true);

                expect(vi.getTimerCount()).toBe(0);
            } finally {
                vi.useRealTimers();
            }
        });

        it("sets no timer for an upload without such a time", async () => {
            vi.useFakeTimers();
            try {
                const request = promiseWithResolvers<object>();
                const { adapter } = createAdapter({ send: vi.fn(() => request.promise) });

                const uploading = adapter.upload("journal", new Uint8Array([1]), "application/octet-stream");
                await vi.advanceTimersByTimeAsync(0);

                expect(vi.getTimerCount()).toBe(0);
                request.resolve({});
                await expect(uploading).resolves.toBe(true);
            } finally {
                vi.useRealTimers();
            }
        });
    });

    it("leaves a request which started at or after the given time untouched", async () => {
        const request = promiseWithResolvers<object>();
        const send = vi.fn(() => request.promise);
        const { adapter } = createAdapter({ send });
        const startedBefore = Date.now();

        const uploading = adapter.upload("file.txt", new Uint8Array([1]), "text/plain");
        await vi.waitFor(() => expect(send).toHaveBeenCalledOnce());

        expect(adapter.abortRequestsStartedBefore(startedBefore)).toBe(0);
        request.resolve({});
        await expect(uploading).resolves.toBe(true);
    });

    it("aborts a download while its response body is still being read", async () => {
        const bodyRead = vi.fn();
        const send = vi.fn((_command: unknown, options?: { abortSignal?: AbortSignal }) =>
            Promise.resolve({
                Body: {
                    transformToByteArray: () => {
                        bodyRead();
                        return rejectWhenAborted<Uint8Array>(options?.abortSignal);
                    },
                },
            })
        );
        const { adapter, requestCount, responseCount } = createAdapter({ send });

        const downloading = adapter.downloadWithResult("file.txt");
        await vi.waitFor(() => expect(bodyRead).toHaveBeenCalledOnce());

        expect(adapter.abortRequestsStartedBefore(Date.now() + 1)).toBe(1);
        await expect(downloading).resolves.toMatchObject({ status: "unavailable" });
        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
    });

    it("aborts a journal listing which is still waiting", async () => {
        const listObjectsV2 = vi.fn((_input: unknown, options?: { abortSignal?: AbortSignal }) =>
            rejectWhenAborted(options?.abortSignal)
        );
        const { adapter, requestCount, responseCount } = createAdapter({ listObjectsV2, send: vi.fn() });

        const listing = adapter.listFiles("");
        await vi.waitFor(() => expect(listObjectsV2).toHaveBeenCalledOnce());

        expect(adapter.abortRequestsStartedBefore(Date.now() + 1)).toBe(1);
        await expect(listing).rejects.toMatchObject({ name: "AbortError" });
        expect(requestCount.value).toBe(1);
        expect(responseCount.value).toBe(1);
    });
});
