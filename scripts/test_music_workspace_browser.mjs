/** Run against the isolated Music Core fixture server, never a user's live Tater. */
import assert from "node:assert/strict";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");
const base = process.argv[2];
assert.ok(
  base && new URL(base).hostname === "127.0.0.1",
  "Pass the isolated fixture URL.",
);
const browser = await chromium.launch({
  headless: true,
  ...(process.env.CHROME_PATH
    ? { executablePath: process.env.CHROME_PATH }
    : {}),
});
const page = await browser.newPage({ viewport: { width: 1050, height: 980 } });
const errors = [],
  requests = [];
page.on("pageerror", (e) => errors.push(e.message));
page.on("console", (message) => {
  if (message.text().includes("[Vue warn]")) errors.push(message.text());
});
page.on("request", (r) => {
  if (r.method() === "POST" && r.url().endsWith("/action"))
    requests.push(r.postDataJSON());
});
try {
  await page.goto(base);
  await page.getByRole("heading", { name: "Music", exact: true }).waitFor();
  const nav = page.getByRole("navigation", { name: "Music navigation" });
  await nav.getByRole("button", { name: "Queue", exact: true }).click();
  const queue = page.getByRole("region", { name: "Current queue" });
  await queue.locator(".tm-track").nth(1).click();
  await page.waitForFunction(
    () =>
      document.querySelector(".tm-dock-track strong")?.textContent ===
      "Blue in Green",
  );
  assert.ok(
    requests.some(
      (r) => r.action === "music_ui_queue_play" && r.payload.id === "queue:1",
    ),
  );
  await page
    .getByRole("slider", { name: "Track position", exact: true })
    .fill("45");
  await page.waitForFunction(
    () =>
      Number(
        document.querySelector('input[aria-label="Track position"]').value,
      ) >= 45,
  );
  assert.equal(
    requests.findLast((r) => r.action === "music_ui_seek").payload.values
      .position_seconds,
    45,
  );
  await page.getByRole("button", { name: "Pause music", exact: true }).click();
  await page
    .getByRole("button", { name: "Resume music", exact: true })
    .waitFor();
  const pausedPosition = await page
    .getByRole("slider", { name: "Track position", exact: true })
    .inputValue();
  await page.waitForTimeout(1250);
  assert.equal(
    await page
      .getByRole("slider", { name: "Track position", exact: true })
      .inputValue(),
    pausedPosition,
  );
  await nav.getByRole("button", { name: "Library", exact: true }).click();
  const before = requests.filter((r) => r.action.includes("_play")).length;
  await page
    .getByRole("button", { name: "Browse Exodus", exact: true })
    .click();
  await page.getByRole("heading", { name: "Exodus", exact: true }).waitFor();
  assert.equal(
    requests.filter((r) => r.action.includes("_play")).length,
    before,
    "Browsing must not start playback",
  );
  await page.locator(".tm-library-tracks .tm-track").first().click();
  await page.waitForFunction(
    () =>
      document.querySelector(".tm-dock-track strong")?.textContent ===
      "Three Little Birds",
  );
  assert.ok(
    requests.some(
      (r) =>
        r.action === "music_ui_track_play" &&
        r.payload.values.track_id === "track:one",
    ),
  );
  await page
    .getByRole("button", { name: "Back to library", exact: true })
    .click();
  await page
    .getByRole("searchbox", { name: "Search music", exact: true })
    .fill("Miles");
  await page
    .getByRole("searchbox", { name: "Search music", exact: true })
    .press("Enter");
  await page
    .getByRole("heading", { name: "Results for “Miles”", exact: true })
    .waitFor();
  assert.equal(await page.locator(".tm-library-tracks .tm-track").count(), 2);
  await page
    .getByRole("searchbox", { name: "Search music", exact: true })
    .fill("Bob");
  await page.getByRole("button", { name: "Play results", exact: true }).click();
  await page.waitForFunction(
    () =>
      document.querySelector(".tm-dock-track strong")?.textContent ===
      "Blue in Green",
  );
  assert.deepEqual(
    requests.findLast((r) => r.action === "music_ui_selection_play").payload
      .values,
    { query: "Miles" },
    "Play results uses the displayed search until a new search is submitted",
  );
  await nav.getByRole("button", { name: "Listen", exact: true }).click();
  await page
    .getByRole("searchbox", {
      name: "Search artists, albums, or songs",
      exact: true,
    })
    .fill("Miles");
  await page
    .getByRole("searchbox", {
      name: "Search artists, albums, or songs",
      exact: true,
    })
    .press("Enter");
  await page
    .getByRole("heading", { name: "Results for “Miles”", exact: true })
    .waitFor();
  await nav.getByRole("button", { name: "Speakers", exact: true }).click();
  const checkers = page
    .locator(".tm-speaker-card")
    .filter({ has: page.getByText("Checkers", { exact: true }) });
  await checkers.getByRole("checkbox").check();
  await checkers
    .getByRole("slider", { name: "Checkers volume", exact: true })
    .fill("61");
  await page.waitForTimeout(1250); // Cross a fixture SSE refresh.
  assert.equal(
    await checkers
      .getByRole("slider", { name: "Checkers volume", exact: true })
      .inputValue(),
    "61",
  );
  await nav.getByRole("button", { name: "Library", exact: true }).click();
  await nav.getByRole("button", { name: "Speakers", exact: true }).click();
  assert.equal(
    await checkers
      .getByRole("slider", { name: "Checkers volume", exact: true })
      .inputValue(),
    "61",
  );
  assert.match(
    await page.locator(".tm-speaker-selection").innerText(),
    /2 destinations.*3 physical speakers/s,
  );
  await page
    .getByRole("button", { name: "Apply to playback", exact: true })
    .click();
  await page.waitForFunction(() =>
    document
      .querySelector(".tm-dock-speakers")
      ?.textContent.includes("2 destinations"),
  );
  const saved = requests.findLast((r) => r.action === "music_ui_save_player");
  assert.deepEqual(saved.payload.values.targets, [
    "voice_core:stereo:office",
    "voice_core:native:checkers",
  ]);
  assert.equal(
    saved.payload.values.player_settings["voice_core:native:checkers"]
      .volume_percent,
    61,
  );
  await page.getByRole("button", { name: "Repeat: off", exact: true }).click();
  await page
    .getByRole("button", { name: "Repeat: all", exact: true })
    .waitFor();
  await nav.getByRole("button", { name: "Settings", exact: true }).click();
  assert.equal(
    await page
      .getByRole("combobox", { name: /Music Profile Person/ })
      .isVisible(),
    false,
  );
  assert.equal(
    await page
      .getByRole("textbox", { name: "Tater Tube Server URL", exact: true })
      .isVisible(),
    false,
  );
  const defaults = page.getByRole("slider", {
    name: "Default Volume",
    exact: true,
  });
  await defaults.fill("39");
  await page.waitForTimeout(1250);
  assert.equal(
    await defaults.inputValue(),
    "39",
    "An SSE refresh must preserve settings drafts",
  );
  await page
    .getByRole("navigation", { name: "Music settings sections" })
    .getByRole("button", { name: "Personalization", exact: true })
    .click();
  await page.getByRole("combobox", { name: /Music Profile Person/ }).waitFor();
  assert.equal(
    await defaults.isVisible(),
    false,
    "Only the selected settings section is visible",
  );
  await page
    .getByRole("navigation", { name: "Music settings sections" })
    .getByRole("button", { name: "Playback", exact: true })
    .click();
  assert.equal(await defaults.inputValue(), "39");
  for (const theme of ["tater", "tater-light"]) {
    await page.evaluate(
      (theme) => (document.documentElement.dataset.theme = theme),
      theme,
    );
    for (const width of [1050, 736, 375, 320]) {
      await page.setViewportSize({ width, height: 980 });
      for (const name of [
        "Listen",
        "Library",
        "Queue",
        "Speakers",
        "Settings",
      ]) {
        await nav.getByRole("button", { name, exact: true }).click();
        const size = await page
          .locator(".tm-music-workspace")
          .evaluate((el) => ({
            width: el.clientWidth,
            scroll: el.scrollWidth,
          }));
        assert.ok(
          size.scroll <= size.width + 2,
          name + " overflows at " + width + ": " + JSON.stringify(size),
        );
      }
    }
  }
  await page.evaluate(() => (document.documentElement.dataset.theme = "tater"));
  await page.setViewportSize({ width: 1050, height: 980 });
  await nav.getByRole("button", { name: "Listen", exact: true }).click();
  if (await page.getByRole("button", { name: "Dismiss message" }).count())
    await page.getByRole("button", { name: "Dismiss message" }).click();
  await page.screenshot({
    path: process.env.MUSIC_QA_SCREENSHOT || "/tmp/tater-music-workspace.png",
    fullPage: true,
  });
  assert.deepEqual(errors, []);
  console.log(
    "PASS Music workspace: browsing, search, queue, speakers, drafts, repeat, and responsive layouts",
  );
} finally {
  await browser.close();
}
