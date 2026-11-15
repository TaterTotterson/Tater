# Core workspaces

The Core library and individual workspaces use the same application theme. Each
built-in Core has a screen recipe in `frontend/src/cores/workspaces.json`, mirrored
by its optional `CORE_WORKSPACE` metadata in Tater Shop. The app supplies defaults
for older installed Cores; community Cores and unknown screens retain the generic
renderer.

## Layouts

- AI Tasks and RSS use a searchable list with selected details and an explicit editor.
- Automation uses a rule list, run timeline, and a vertical trigger/condition/action/audio editor.
- Awareness uses an event timeline with media previews and monitored source details.
- Environment uses native current conditions, space readings, forecasts, and source context.
- Guardian uses a readable briefing, device review, and network event timeline.
- Memory uses remembered context and people lists, with insights and data tools.
- Meshtastic keeps channel messaging beside radio context, nodes, and pairing controls.
- Music keeps its library and persistent player, adding the upcoming queue beside the content.
- Personal uses an agenda with context and account management.
- Reachy keeps per-device behavior controls and existing behavior groups.
- Tater Tube uses media context, pick cards, and viewing history.

Screen recipes only affect presentation. The original field records, actions,
payloads, popup settings, selection, pagination, and backend functions remain the
source of truth. New or unrecognized fields remain in the editor. Credentials and
private raw data stay out of summary previews. Existing visibility conditions are
honored. Source weather tables and generated charts remain accessible in
disclosures when their readings also appear in the native overview.

## Validation

Run the Node workspace tests and the existing backend/UI regressions, then build
the production assets with `npm --prefix frontend run build`. The Tater Shop test
`tests.test_core_workspace_metadata` validates screen metadata, versions, and
manifest hashes.

`frontend/test-fixtures/core-workspaces.html` mounts actual production components
with synthetic data and mocked action endpoints. It is a development fixture and
does not enter the production bundle. Start Vite on port 5179 and run
`node scripts/test_core_workspace_browser.mjs` with Playwright available. Set
`TATER_PLAYWRIGHT_MODULE` to a module path when using an external runtime, and
`TATER_WORKSPACE_SCREENSHOTS` to save visual previews. Browser checks cover all
twelve Cores at desktop, tablet, and phone widths, light and dark presentation,
refresh-safe edits, action payloads, radio sending, and private field access.

Release both the app UI assets and the matching Shop Core versions to distribute
the complete update. A macOS test-app build and release are separate steps.
