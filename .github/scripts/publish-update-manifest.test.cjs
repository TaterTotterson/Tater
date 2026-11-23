const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test, mock } = require("node:test");
const { publishUpdateManifest } = require("./publish-update-manifest.cjs");

const manifest = {
  version: "3.0.9", build: 233,
  url: "https://github.com/TaterTotterson/Tater/releases/download/v3.0.9/Tater-v3.0.9.zip",
  sha256: "a".repeat(64), notes: "Tater macOS update v3.0.9.",
};

function fixture(t, existing = { ...manifest, version: "3.0.8", build: 232 }) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "tater-updater-test-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const manifestPath = path.join(directory, "manifest.json");
  fs.writeFileSync(manifestPath, JSON.stringify(manifest));
  const release = {
    draft: false, prerelease: false,
    assets: [{ name: "Tater-v3.0.9.zip", state: "uploaded",
               browser_download_url: manifest.url, digest: `sha256:${manifest.sha256}` }],
  };
  const remote = (value, sha = "current-file-sha") => ({ data: {
    sha, content: Buffer.from(JSON.stringify(value)).toString("base64"),
  } });
  const repos = {
    getReleaseByTag: mock.fn(async () => ({ data: release })),
    getContent: mock.fn(async () => remote(existing)),
    createOrUpdateFileContents: mock.fn(async () => ({})),
  };
  return {
    repos, remote, release,
    args: { github: { rest: { repos } }, context: { repo: { owner: "TaterTotterson", repo: "Tater" },
            ref: "refs/tags/v3.0.9" }, core: { info: () => {} }, manifestPath },
  };
}

test("publishes against main's current file even when main has advanced past the release tag", async (t) => {
  const { args, repos } = fixture(t);
  await publishUpdateManifest(args);
  const update = repos.createOrUpdateFileContents.mock.calls[0].arguments[0];
  assert.equal(repos.getContent.mock.calls[0].arguments[0].ref, "main");
  assert.equal(update.branch, "main");
  assert.equal(update.sha, "current-file-sha");
  assert.deepEqual(JSON.parse(Buffer.from(update.content, "base64")), manifest);
});

test("refreshes the file SHA and retries after a concurrent commit", async (t) => {
  const { args, repos, remote } = fixture(t);
  let reads = 0;
  let writes = 0;
  repos.getContent = mock.fn(async () => remote({ ...manifest, build: 232 },
    ++reads === 1 ? "first-sha" : "second-sha"));
  repos.createOrUpdateFileContents = mock.fn(async () => {
    if (++writes === 1) throw Object.assign(new Error("Conflict"), { status: 409 });
  });
  await publishUpdateManifest(args);
  assert.equal(repos.createOrUpdateFileContents.mock.callCount(), 2);
  assert.equal(repos.createOrUpdateFileContents.mock.calls[1].arguments[0].sha, "second-sha");
});

test("an older release cannot replace a newer updater feed", async (t) => {
  for (const newer of [{ ...manifest, version: "3.0.10" }, { ...manifest, build: 234 }]) {
    const { args, repos } = fixture(t, newer);
    await publishUpdateManifest(args);
    assert.equal(repos.createOrUpdateFileContents.mock.callCount(), 0);
  }
});

test("rechecks for a newer release after a conflict", async (t) => {
  const { args, repos, remote } = fixture(t);
  let reads = 0;
  repos.getContent = mock.fn(async () => remote({ ...manifest,
    build: ++reads === 1 ? 232 : 234 }));
  repos.createOrUpdateFileContents = mock.fn(async () => { throw Object.assign(new Error("Conflict"), { status: 409 }); });
  await publishUpdateManifest(args);
  assert.equal(repos.createOrUpdateFileContents.mock.callCount(), 1);
});

test("an already current feed does not create another commit", async (t) => {
  const { args, repos } = fixture(t, manifest);
  await publishUpdateManifest(args);
  assert.equal(repos.createOrUpdateFileContents.mock.callCount(), 0);
});

test("does not advertise an unpublished or mismatched updater archive", async (t) => {
  for (const kind of ["missing", "digest", "draft"]) {
    const { args, repos, release } = fixture(t);
    if (kind === "missing") release.assets = [];
    if (kind === "digest") release.assets[0].digest = `sha256:${"b".repeat(64)}`;
    if (kind === "draft") release.draft = true;
    await assert.rejects(publishUpdateManifest(args), /archive is missing or does not match/);
    assert.equal(repos.createOrUpdateFileContents.mock.callCount(), 0);
  }
});

test("publication errors fail the release step instead of silently leaving an old feed", async (t) => {
  const { args, repos } = fixture(t);
  repos.createOrUpdateFileContents = mock.fn(async () => { throw Object.assign(new Error("Permission denied"), { status: 403 }); });
  await assert.rejects(publishUpdateManifest(args), /Permission denied/);
  assert.equal(repos.createOrUpdateFileContents.mock.callCount(), 1);
});

test("stops after three conflicts and reports the failure", async (t) => {
  const { args, repos } = fixture(t);
  repos.createOrUpdateFileContents = mock.fn(async () => { throw Object.assign(new Error("Conflict"), { status: 409 }); });
  await assert.rejects(publishUpdateManifest(args), /Conflict/);
  assert.equal(repos.createOrUpdateFileContents.mock.callCount(), 3);
});
