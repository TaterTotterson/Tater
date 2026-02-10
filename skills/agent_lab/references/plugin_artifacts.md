# Plugin Pattern: Artifacts (Image/Audio/Video/File)

Use this when plugins return media or downloadable files.

## Artifact Shape
Each artifact is a dict with:
- `type`: `image|audio|video|file`
- `name`: filename
- `mimetype`: media type
- `bytes` or `blob_key`

## Success Payload Example
```python
return action_success(
    facts={"count": 1},
    say_hint="Generated one image.",
    artifacts=[
        {
            "type": "image",
            "name": "result.png",
            "mimetype": "image/png",
            "bytes": image_bytes,
        }
    ],
)
```

## Notes
- Use `bytes` for freshly generated content.
- Use `blob_key` when content is already stored.
- Keep `say_hint` short and specific to returned artifacts.
