const fs = require("node:fs");

function releaseParts(manifest) {
  const version = String(manifest.version || "");
  if (!/^\d+(?:\.\d+)*$/.test(version) || !Number.isSafeInteger(manifest.build) || manifest.build < 0) {
    throw new Error("Invalid updater version or build number.");
  }
  const parts = version.split(".").map(Number);
  if (!parts.every(Number.isSafeInteger)) throw new Error("Invalid updater version.");
  return parts;
}

function compareReleases(left, right) {
  const a = releaseParts(left);
  const b = releaseParts(right);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const difference = (a[index] || 0) - (b[index] || 0);
    if (difference) return difference;
  }
  return left.build - right.build;
}

async function publishUpdateManifest({ github, context, core, manifestPath = process.env.MANIFEST_PATH }) {
  const content = fs.readFileSync(manifestPath, "utf8");
  const manifest = JSON.parse(content);
  releaseParts(manifest);
  const { owner, repo } = context.repo;
  const tag = String(context.ref || "").replace(/^refs\/tags\//, "");
  const expectedURL = `https://github.com/${owner}/${repo}/releases/download/${tag}/Tater-${tag}.zip`;
  if (tag !== `v${manifest.version}` || manifest.url !== expectedURL || !/^[a-f0-9]{64}$/.test(manifest.sha256)) {
    throw new Error("Updater manifest does not match this release tag and archive.");
  }

  // Advertise only an archive that has already been published successfully.
  const { data: release } = await github.rest.repos.getReleaseByTag({ owner, repo, tag });
  const asset = release.assets.find((row) => row.name === `Tater-${tag}.zip`);
  if (release.draft || release.prerelease || !asset || asset.state !== "uploaded"
      || asset.browser_download_url !== manifest.url
      || (asset.digest && asset.digest !== `sha256:${manifest.sha256}`)) {
    throw new Error("The published updater archive is missing or does not match the manifest.");
  }

  const path = "macos/Tater/update-manifest.json";
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    let current;
    try {
      ({ data: current } = await github.rest.repos.getContent({ owner, repo, path, ref: "main" }));
    } catch (error) {
      if (error.status !== 404) throw error;
    }
    if (current) {
      const existing = JSON.parse(Buffer.from(current.content, "base64").toString("utf8"));
      if (compareReleases(existing, manifest) > 0) {
        core.info("A newer macOS updater manifest is already published; keeping it.");
        return;
      }
      if (JSON.stringify(existing) === JSON.stringify(manifest)) {
        core.info("The macOS updater manifest is already current.");
        return;
      }
    }
    try {
      await github.rest.repos.createOrUpdateFileContents({
        owner, repo, path, branch: "main",
        message: `Update macOS release manifest for ${tag}`,
        content: Buffer.from(content).toString("base64"),
        ...(current ? { sha: current.sha } : {}),
      });
      core.info(`Published the macOS updater manifest for ${tag}.`);
      return;
    } catch (error) {
      if (![409, 422].includes(error.status) || attempt === 3) throw error;
      core.info("Main changed during updater publication; reading the latest manifest and retrying.");
    }
  }
}

module.exports = { publishUpdateManifest };
