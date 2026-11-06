const fs = require("node:fs");

function cleanTitle(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/([\[\]])/g, "\\$1");
}

function externalContributorGroups(pullRequests, owner) {
  const ownerLogin = String(owner || "").toLowerCase();
  const groups = new Map();

  for (const pullRequest of pullRequests || []) {
    const login = String(pullRequest?.user?.login || "").trim();
    const userType = String(pullRequest?.user?.type || "").trim().toLowerCase();
    if (
      !login ||
      login.toLowerCase() === ownerLogin ||
      userType === "bot" ||
      login.toLowerCase().endsWith("[bot]")
    ) {
      continue;
    }

    const key = login.toLowerCase();
    if (!groups.has(key)) {
      groups.set(key, { login, pullRequests: [] });
    }
    groups.get(key).pullRequests.push({
      number: Number(pullRequest.number),
      title: cleanTitle(pullRequest.title),
      html_url: String(pullRequest.html_url || "").trim(),
    });
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      pullRequests: group.pullRequests.sort((left, right) => left.number - right.number),
    }))
    .sort((left, right) => left.login.localeCompare(right.login, undefined, { sensitivity: "base" }));
}

function buildContributorSection(pullRequests, owner) {
  const groups = externalContributorGroups(pullRequests, owner);
  if (!groups.length) return "";

  const lines = [groups.length === 1 ? "## Contributor" : "## Contributors", ""];
  for (const [index, group] of groups.entries()) {
    if (index) lines.push("");
    lines.push(`Contributions from @${group.login} included in this release:`, "");
    for (const pullRequest of group.pullRequests) {
      lines.push(`- [#${pullRequest.number} — ${pullRequest.title}](${pullRequest.html_url})`);
    }
  }
  return lines.join("\n");
}

function insertContributorSection(releaseNotes, contributorSection) {
  const notes = String(releaseNotes || "").trimEnd();
  const section = String(contributorSection || "").trim();
  if (!section) return `${notes}\n`;

  const changelogMatch = /^\*\*Full Changelog\*\*:/m.exec(notes);
  if (!changelogMatch) return `${notes}\n\n${section}\n`;

  const before = notes.slice(0, changelogMatch.index).trimEnd();
  const after = notes.slice(changelogMatch.index).trimStart();
  return `${before}\n\n${section}\n\n${after}\n`;
}

async function commitsInRelease({ github, owner, repo, previousTag, currentTag }) {
  const commits = [];
  for (let page = 1; page <= 3; page += 1) {
    const response = await github.rest.repos.compareCommitsWithBasehead({
      owner,
      repo,
      basehead: `${previousTag}...${currentTag}`,
      per_page: 100,
      page,
    });
    const pageCommits = Array.isArray(response.data?.commits) ? response.data.commits : [];
    commits.push(...pageCommits);
    if (pageCommits.length < 100) break;
  }
  return commits;
}

async function pullRequestsInRelease({ github, owner, repo, previousTag, currentTag, defaultBranch }) {
  const commits = await commitsInRelease({ github, owner, repo, previousTag, currentTag });
  const pullRequests = new Map();

  for (const commit of commits) {
    const associated = await github.paginate(
      github.rest.repos.listPullRequestsAssociatedWithCommit,
      { owner, repo, commit_sha: commit.sha, per_page: 100 },
    );
    for (const pullRequest of associated) {
      if (
        pullRequest.merged_at &&
        pullRequest.base?.ref === defaultBranch &&
        !pullRequests.has(pullRequest.number)
      ) {
        pullRequests.set(pullRequest.number, pullRequest);
      }
    }
  }

  return [...pullRequests.values()].sort((left, right) => left.number - right.number);
}

async function renderReleaseNotes({ github, context, core }) {
  const sourcePath = process.env.RELEASE_NOTES_PATH;
  const outputPath = process.env.RENDERED_RELEASE_NOTES_PATH;
  const previousTag = process.env.PREVIOUS_TAG;
  const currentTag = process.env.VERSION_LABEL;
  if (!sourcePath || !outputPath || !previousTag || !currentTag) {
    throw new Error("Release-note contributor rendering is missing a required path or tag.");
  }

  const { owner, repo } = context.repo;
  const repository = await github.rest.repos.get({ owner, repo });
  const pullRequests = await pullRequestsInRelease({
    github,
    owner,
    repo,
    previousTag,
    currentTag,
    defaultBranch: repository.data.default_branch,
  });
  const contributorSection = buildContributorSection(pullRequests, owner);
  const releaseNotes = fs.readFileSync(sourcePath, "utf8");
  fs.writeFileSync(outputPath, insertContributorSection(releaseNotes, contributorSection), "utf8");

  const contributors = externalContributorGroups(pullRequests, owner);
  if (contributors.length) {
    core.info(
      `Added ${contributors.length} external contributor(s) and ` +
        `${contributors.reduce((total, group) => total + group.pullRequests.length, 0)} pull request(s).`,
    );
  } else {
    core.info("No external contributor pull requests were found for this release.");
  }
}

module.exports = {
  buildContributorSection,
  externalContributorGroups,
  insertContributorSection,
  renderReleaseNotes,
};
