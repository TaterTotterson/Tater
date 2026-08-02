#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueChatTests(unittest.TestCase):
    def test_chat_uses_shared_versioned_vue_bundle_with_legacy_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVueChat", app_js)
        self.assertIn("async function loadVueChatView", app_js)
        self.assertIn("module.mountChat", app_js)
        self.assertIn('withBasePath("/api/chat/jobs")', app_js)
        self.assertIn('withBasePath("/api/chat/files")', app_js)
        self.assertIn("The Vue Chat surface could not load; using the legacy renderer.", app_js)
        self.assertIn("await loadChatView();", app_js)
        self.assertIn("export function mountChat", entry)

    def test_chat_preserves_streaming_polling_attachments_and_session_identity(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "chat" / "ChatApp.vue").read_text(encoding="utf-8")

        for feature in (
            "new EventSource",
            'addEventListener("response_chunk"',
            'addEventListener("waiting"',
            'addEventListener("job_error"',
            "schedulePoll",
            "new FileReader",
            "data_url",
            "session_id: sessionId.value",
            "onSessionChange",
            "event.shiftKey",
            "event.isComposing",
        ):
            self.assertIn(feature, source)
        self.assertEqual(source.count('ref="fileInput"'), 1)

    def test_chat_renders_safe_rich_messages_and_full_height_layout(self) -> None:
        message = (REPO_ROOT / "frontend" / "src" / "chat" / "components" / "ChatMessage.vue").read_text(encoding="utf-8")
        markdown = (REPO_ROOT / "frontend" / "src" / "chat" / "markdown.ts").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for message_type in ("contentType === 'image'", "contentType === 'audio'", "contentType === 'video'", "contentType === 'file'"):
            self.assertIn(message_type, message)
        self.assertIn("v-html=\"renderMarkdown(plainContent)\"", message)
        self.assertIn('lowered.startsWith("https://")', markdown)
        self.assertIn('return "";', markdown)
        self.assertIn('.view-root[data-view="chat"] > .tater-chat-mount', styles)
        self.assertIn('.view-root[data-view="chat"] .tc-composer-card { order: 4;', styles)
        self.assertIn(".tc-chat-log { order: 1; min-height: 0; max-height: none; flex: 1;", styles)


if __name__ == "__main__":
    unittest.main()
