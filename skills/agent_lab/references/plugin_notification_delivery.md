# Plugin Pattern: Notification Delivery

Use this when plugins send outbound messages/alerts to rooms/channels.

## Core Rules
- Accept friendly target names (for example `#tater`) when possible.
- Preserve and pass `origin` so downstream routing can default intelligently.
- Normalize platform + targets before dispatch.

## Recommended Args Shape
```json
{"platform":"discord","targets":{"channel":"#alerts"},"message":"Hello"}
```

## Reliability Notes
- Validate destination platform before dispatch.
- Return actionable `action_failure` when target/platform is missing.
- Keep success narration short and explicit about destination.

## Attachments
When supported, pass attachments as:
- `type` (`image|audio|video|file`)
- `name`, `mimetype`
- `bytes` or `blob_key`
