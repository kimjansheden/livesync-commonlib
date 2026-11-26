import { afterEach, describe, expect, it, vi } from "vitest";
import { DOCID_JOURNAL_SYNC_PARAMETERS, ProtocolVersions, type PlainEntry } from "@lib/common/types.ts";
import { LocalChangeSendOutcomes } from "@lib/replication/LiveSyncAbstractReplicator.ts";
import {
    closeDevices,
    createDevice,
    createRemote,
    encoder,
    entriesOf,
    note,
    storedEntries,
    type Device,
    type Remote,
} from "./journalSpecHarness.spec.ts";

afterEach(closeDevices);

/** A remote with sync parameters, which the check before each cycle reads. */
function createWorld() {
    const remote = createRemote();
    remote.put(
        DOCID_JOURNAL_SYNC_PARAMETERS,
        encoder.encode(
            JSON.stringify({ protocolVersion: ProtocolVersions.ADVANCED_E2EE, pbkdf2salt: btoa("synthetic-seed") })
        )
    );
    return { remote, device: (name: string) => createDevice(name, remote.storage) };
}

/** What a host does in one cycle: the check of the remote and of its sync parameters, then the synchronisation. */
async function cycle(device: Device) {
    await device.core.ensureCheckpointCachesAreFresh();
    return await device.core.sync();
}

/** A computer and a phone which have both synchronised the document `shared` through the remote. */
async function phoneAndComputer() {
    const world = createWorld();
    const computer = world.device("computer");
    const phone = world.device("phone");
    await computer.db.put(note("shared"));
    await expect(cycle(computer)).resolves.toBe(true);
    await expect(cycle(phone)).resolves.toBe(true);
    return { ...world, computer, phone };
}

/** Let a device write a document and synchronise. Returns the journal it uploaded. */
async function writeAndSynchronise(remote: Remote, device: Device, id: string) {
    const before = new Set(remote.keys());
    await device.db.put(note(id));
    await expect(cycle(device)).resolves.toBe(true);
    return remote.keys().find((key) => !before.has(key))!;
}

/** The uploads and downloads of journals since the transfer numbered `from`, as `upload` or `download` and a key. */
const journalTransfers = (remote: Remote, from = 0) =>
    remote.transfers
        .slice(from)
        .map((entry) => entry.split(" ") as ["upload" | "download", string])
        .filter(([, key]) => !key.startsWith("_"));
const kinds = (remote: Remote, from: number) => journalTransfers(remote, from).map(([kind]) => kind);

/** Hold every download of a journal until it is released, as a slow connection does. */
function holdJournalDownloads(remote: Remote) {
    let release = () => {};
    const released = new Promise<void>((resolve) => (release = resolve));
    const held: string[] = [];
    const download = remote.downloadWithResult.getMockImplementation()!;
    remote.downloadWithResult.mockImplementation(async (key) => {
        if (!key.startsWith("_")) {
            held.push(key);
            await released;
        }
        return await download(key);
    });
    return { held, release };
}

const hasApplied = (device: Device, id: string) => device.handedOn.some((entry) => entry.startsWith(`${id}@`));

async function edit(device: Device, id: string, mtime: number) {
    const doc = (await device.db.get(id)) as PlainEntry;
    await device.db.put({ ...doc, mtime });
}

/** The cleaner of a device, which a synchronisation runs last. */
const cleanerOf = (device: Device) =>
    (device.core as unknown as { cleaner: { run: (listing: unknown) => Promise<void> } }).cleaner;

describe("JournalSyncCore order of sending and receiving", () => {
    it("sends a local change before a slow receive of the journals with documents has finished", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        await writeAndSynchronise(remote, computer, "from-computer");
        await phone.db.put(note("from-phone"));
        const downloads = holdJournalDownloads(remote);

        const synchronised = cycle(phone);
        try {
            await vi.waitFor(() => expect(downloads.held).toHaveLength(1));
            // The change is on the remote while the journal of the other device is still on its way.
            expect((await storedEntries(remote)).has("from-phone")).toBe(true);
            expect(hasApplied(phone, "from-computer")).toBe(false);
        } finally {
            downloads.release();
        }

        await expect(synchronised).resolves.toBe(true);
        expect(hasApplied(phone, "from-computer")).toBe(true);
        expect((await storedEntries(remote)).get("from-phone")).toBe(1);
    });

    it("sends a change made during the receive at the end of the same synchronisation, after the one made before", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        const fromComputer = await writeAndSynchronise(remote, computer, "from-computer");
        await phone.db.put(note("before-the-receive"));
        let written = false;
        phone.core.processReplication = async () => {
            if (!written) await phone.db.put(note("during-the-receive"));
            written = true;
            return true;
        };
        const from = remote.transfers.length;

        await expect(cycle(phone)).resolves.toBe(true);

        const transfers = journalTransfers(remote, from);
        expect(transfers.map(([kind]) => kind)).toEqual(["upload", "download", "upload"]);
        expect(transfers[1][1]).toBe(fromComputer);
        await expect(entriesOf(remote, transfers[0][1])).resolves.toEqual({
            chunks: [],
            documents: ["before-the-receive"],
        });
        await expect(entriesOf(remote, transfers[2][1])).resolves.toEqual({
            chunks: [],
            documents: ["during-the-receive"],
        });
        expect(phone.checkpoint.lastLocalSeq).toBe((await phone.db.info()).update_seq);
    });

    it("receives first in a synchronisation which did not begin with the check of the sync parameters", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        await writeAndSynchronise(remote, computer, "from-computer");
        // Another client on the same store has not read the sync parameters of the remote in this process.
        const client = phone.createPaneClient();
        await phone.db.put(note("from-phone"));
        const from = remote.transfers.length;

        await expect(client.sync()).resolves.toBe(true);

        expect(kinds(remote, from)).toEqual(["download", "upload"]);
    });
});

describe("JournalSyncCore safeguard before sending ahead of the receive", () => {
    it("sends nothing when the listing fails, also when a send is asked for during it", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        // The remote changed, so the phone lists it again instead of reusing its listing.
        await writeAndSynchronise(remote, computer, "from-computer");
        await phone.db.put(note("from-phone"));
        let failListing = () => {};
        const listingFails = new Promise<void>((resolve) => (failListing = resolve));
        remote.storage.listObjectsInUploadOrder.mockImplementationOnce(async () => {
            await listingFails;
            throw new Error("synthetic listing failure");
        });
        const uploads = remote.upload.mock.calls.length;
        const listings = remote.storage.listObjectsInUploadOrder.mock.calls.length;

        const synchronised = cycle(phone);
        await vi.waitFor(() =>
            expect(remote.storage.listObjectsInUploadOrder.mock.calls.length).toBeGreaterThan(listings)
        );
        const asked = phone.core.sendLocalChangesNow();
        failListing();

        await expect(synchronised).resolves.toBe(false);
        await expect(asked).resolves.toBe(LocalChangeSendOutcomes.NEEDS_REPLICATION);
        expect(remote.upload.mock.calls.length).toBe(uploads);
        expect((await storedEntries(remote)).has("from-phone")).toBe(false);
    });

    it("receives first when the remote holds no journal of this device's history any more", async () => {
        const world = createWorld();
        const { remote } = world;
        const phone = world.device("phone");
        await writeAndSynchronise(remote, phone, "from-phone-earlier");
        // The remote is emptied and filled again by another device, under the same sync parameters.
        await remote.storage.deleteFiles(remote.keys());
        const rebuilt = await writeAndSynchronise(remote, world.device("rebuilder"), "after-the-rebuild");
        await phone.db.put(note("from-phone"));
        const from = remote.transfers.length;
        const downloads = holdJournalDownloads(remote);

        const synchronised = cycle(phone);
        try {
            await vi.waitFor(() => expect(downloads.held).toEqual([rebuilt]));
            await expect(phone.core.sendLocalChangesNow()).resolves.toBe(LocalChangeSendOutcomes.NEEDS_REPLICATION);
            // Nothing is uploaded while the journal of the rebuilt remote is on its way.
            expect(kinds(remote, from)).not.toContain("upload");
        } finally {
            downloads.release();
        }

        await expect(synchronised).resolves.toBe(true);
        expect(kinds(remote, from)).toEqual(["download", "upload"]);
    });
});

describe("JournalSyncCore send of the local changes now", () => {
    it("completes a send which is asked for during the receive without waiting for the receive", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        await writeAndSynchronise(remote, computer, "from-computer");
        const downloads = holdJournalDownloads(remote);

        const synchronised = cycle(phone);
        try {
            await vi.waitFor(() => expect(downloads.held).toHaveLength(1));
            await phone.db.put(note("saved-during-the-receive"));

            await expect(phone.core.sendLocalChangesNow()).resolves.toBe(LocalChangeSendOutcomes.SENT);

            expect((await storedEntries(remote)).has("saved-during-the-receive")).toBe(true);
            expect(hasApplied(phone, "from-computer")).toBe(false);
        } finally {
            downloads.release();
        }

        await expect(synchronised).resolves.toBe(true);
        // The synchronisation's own send after the receive found nothing more to send.
        expect((await storedEntries(remote)).get("saved-during-the-receive")).toBe(1);
        expect(phone.checkpoint.lastLocalSeq).toBe((await phone.db.info()).update_seq);
    });

    it("answers at once, without a request to the remote, when nothing waits to be sent", async () => {
        const { remote, phone } = await phoneAndComputer();
        const from = remote.transfers.length;

        await expect(phone.core.sendLocalChangesNow()).resolves.toBe(LocalChangeSendOutcomes.NOTHING_TO_SEND);

        expect(remote.transfers.length).toBe(from);
    });

    it("leaves a change to a synchronisation when none is running, and transfers nothing", async () => {
        const { remote, phone } = await phoneAndComputer();
        await phone.db.put(note("from-phone"));
        const from = remote.transfers.length;

        await expect(phone.core.sendLocalChangesNow()).resolves.toBe(LocalChangeSendOutcomes.NEEDS_REPLICATION);

        expect(remote.transfers.length).toBe(from);
        await expect(cycle(phone)).resolves.toBe(true);
        expect((await storedEntries(remote)).has("from-phone")).toBe(true);
    });

    it("does not upload again what a receive stores beside it", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        await writeAndSynchronise(remote, computer, "from-computer");
        let asked: Promise<unknown> | undefined;
        phone.core.processReplication = async () => {
            if (!asked) {
                // A save comes while the received revision is stored and not yet registered as known.
                await phone.db.put(note("saved-while-applying"));
                asked = phone.core.sendLocalChangesNow();
                // A handler which takes a while gives the send time to read the local changes.
                await new Promise((resolve) => setTimeout(resolve, 20));
            }
            return true;
        };

        await expect(cycle(phone)).resolves.toBe(true);

        await expect(asked).resolves.toBe(LocalChangeSendOutcomes.SENT);
        const entries = await storedEntries(remote);
        expect(entries.get("from-computer")).toBe(1);
        expect(entries.get("saved-while-applying")).toBe(1);
    });
});

describe("JournalSyncCore cleaning after sending ahead of the receive", () => {
    it("cleans only after every journal with documents was received and the local changes were sent", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        await writeAndSynchronise(remote, computer, "from-computer");
        await phone.db.put(note("from-phone"));
        const seen: { received: boolean; sent: boolean }[] = [];
        vi.spyOn(cleanerOf(phone), "run").mockImplementation(async () => {
            const checkpoint = await phone.core.getCheckpointInfo();
            seen.push({
                received: hasApplied(phone, "from-computer"),
                sent: checkpoint.lastLocalSeq === (await phone.db.info()).update_seq,
            });
        });

        await expect(cycle(phone)).resolves.toBe(true);

        expect(seen).toEqual([{ received: true, sent: true }]);
    });

    it("does not clean when the receive fails after the local changes were sent", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        const fromComputer = await writeAndSynchronise(remote, computer, "from-computer");
        remote.unavailable.add(fromComputer);
        await phone.db.put(note("from-phone"));
        const run = vi.spyOn(cleanerOf(phone), "run");

        await expect(cycle(phone)).resolves.toBe(false);

        expect((await storedEntries(remote)).has("from-phone")).toBe(true);
        expect(run).not.toHaveBeenCalled();
        expect(phone.checkpoint.receivedFiles.has(fromComputer)).toBe(false);
    });
});

describe("JournalSyncCore conflicts after sending ahead of the receive", () => {
    /** The winning revision and the conflicting ones, with the time each one was made. */
    async function leavesOf(device: Device, id: string) {
        const winner = await device.db.get(id, { conflicts: true });
        const conflicts = await Promise.all(
            (winner._conflicts ?? []).map(async (rev) => (await device.db.get(id, { rev })) as PlainEntry)
        );
        return {
            winner: winner._rev,
            leaves: [winner as PlainEntry, ...conflicts].map((doc) => doc.mtime).sort(),
        };
    }

    it("leaves the same detected conflict on both devices when each edited the same document", async () => {
        const { remote, computer, phone } = await phoneAndComputer();
        await edit(computer, "shared", 2);
        await expect(cycle(computer)).resolves.toBe(true);
        await edit(phone, "shared", 3);
        const from = remote.transfers.length;

        await expect(cycle(phone)).resolves.toBe(true);
        await expect(cycle(computer)).resolves.toBe(true);

        // The phone sent its edit before it received the other one.
        expect(kinds(remote, from).slice(0, 2)).toEqual(["upload", "download"]);
        const onPhone = await leavesOf(phone, "shared");
        expect(onPhone.leaves).toEqual([2, 3]);
        await expect(leavesOf(computer, "shared")).resolves.toEqual(onPhone);
    });
});
