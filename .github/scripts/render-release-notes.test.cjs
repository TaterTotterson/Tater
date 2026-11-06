const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  buildContributorSection,
  insertContributorSection,
  renderReleaseNotes,
} = require("./render-release-notes.cjs");

test("renders external contributors and their pull requests", () => {
  const section = buildContributorSection(
    [
      {
        number: 57,
        title: "Default a shallower bass floor on larger Echo speakers",
        html_url: "https://github.com/TaterTotterson/Tater/pull/57",
        user: { login: "MartyTremblay", type: "User" },
      },
      {
        number: 58,
        title: "Owner maintenance",
        html_url: "https://github.com/TaterTotterson/Tater/pull/58",
        user: { login: "TaterTotterson", type: "User" },
      },
      {
        number: 59,
        title: "Automated maintenance",
        html_url: "https://github.com/TaterTotterson/Tater/pull/59",
        user: { login: "dependabot[bot]", type: "Bot" },
      },
    ],
    "TaterTotterson",
  );

  assert.match(section, /^## Contributor$/m);
  assert.match(section, /Contributions from @MartyTremblay included in this release:/);
  assert.match(section, /\[#57 — Default a shallower bass floor on larger Echo speakers\]/);
  assert.doesNotMatch(section, /Owner maintenance/);
  assert.doesNotMatch(section, /Automated maintenance/);
});

test("inserts contributor section before the full changelog", () => {
  const rendered = insertContributorSection(
    "# Tater v2.0.5\n\n## What's New\n\n- A fix.\n\n**Full Changelog**: compare\n",
    "## Contributor\n\nContributions from @MartyTremblay included in this release:",
  );

  assert.ok(rendered.indexOf("## Contributor") < rendered.indexOf("**Full Changelog**"));
  assert.match(rendered, /@MartyTremblay/);
});

test("discovers, deduplicates, and renders merged pull requests in the release range", async () => {
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "tater-release-notes-"));
  const sourcePath = path.join(temporaryDirectory, "source.md");
  const outputPath = path.join(temporaryDirectory, "rendered.md");
  fs.writeFileSync(sourcePath, "# Tater v2.0.5\n\n**Full Changelog**: compare\n", "utf8");

  const previousEnvironment = {
    RELEASE_NOTES_PATH: process.env.RELEASE_NOTES_PATH,
    RENDERED_RELEASE_NOTES_PATH: process.env.RENDERED_RELEASE_NOTES_PATH,
    PREVIOUS_TAG: process.env.PREVIOUS_TAG,
    VERSION_LABEL: process.env.VERSION_LABEL,
  };
  Object.assign(process.env, {
    RELEASE_NOTES_PATH: sourcePath,
    RENDERED_RELEASE_NOTES_PATH: outputPath,
    PREVIOUS_TAG: "v2.0.4",
    VERSION_LABEL: "v2.0.5",
  });

  const pullRequest = {
    number: 57,
    title: "Default a shallower bass floor on larger Echo speakers",
    html_url: "https://github.com/TaterTotterson/Tater/pull/57",
    merged_at: "2026-11-04T13:31:35Z",
    base: { ref: "main" },
    user: { login: "MartyTremblay", type: "User" },
  };
  const github = {
    paginate: async () => [pullRequest],
    rest: {
      repos: {
        compareCommitsWithBasehead: async () => ({
          data: { commits: [{ sha: "first" }, { sha: "second" }] },
        }),
        get: async () => ({ data: { default_branch: "main" } }),
        listPullRequestsAssociatedWithCommit: async () => [],
      },
    },
  };

  try {
    await renderReleaseNotes({
      github,
      context: { repo: { owner: "TaterTotterson", repo: "Tater" } },
      core: { info: () => {} },
    });
    const rendered = fs.readFileSync(outputPath, "utf8");
    assert.equal((rendered.match(/@MartyTremblay/g) || []).length, 1);
    assert.equal((rendered.match(/#57/g) || []).length, 1);
  } finally {
    for (const [key, value] of Object.entries(previousEnvironment)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
});
