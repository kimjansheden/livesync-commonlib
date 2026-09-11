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

    it("preserves an object-store failure in the detailed download result", async () => {
        const failure = new Error("network failed");
        const { adapter } = createAdapter({ send: vi.fn(() => Promise.reject(failure)) });

        await expect(adapter.downloadWithResult("settings.json")).resolves.toEqual({
            status: "unavailable",
            error: failure,
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
