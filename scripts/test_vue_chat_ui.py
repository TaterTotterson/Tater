#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueChatTests(unittest.TestCase):
    def test_chat_is_loaded_by_the_shared_vue_shell(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")

        self.assertIn('import ChatApp from "../chat/ChatApp.vue"', shell)
        self.assertIn("chat: ChatApp", shell)
        self.assertIn('if (view === "chat")', app_js)
        self.assertIn("createChatVueDescriptor", app_js)
        self.assertIn('withBasePath("/api/chat/jobs")', app_js)
        self.assertIn('withBasePath("/api/chat/files")', app_js)
        self.assertNotIn("mountVueChat", app_js)
        self.assertNotIn("loadChatView", app_js)
        self.assertNotIn("legacy renderer", app_js)

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

    def test_assistant_stream_uses_a_smooth_batched_reveal(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "chat" / "ChatApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "base.css").read_text(encoding="utf-8")

        self.assertIn("const STREAM_REVEAL_FRAME_MS = 32", source)
        self.assertIn("appendStreamChunk(jobId, chunk)", source)
        self.assertIn("streamRevealEnd", source)
        self.assertIn("flushStreamReveal(jobId)", source)
        self.assertIn('activeCount && !streamRows.length', source)
        self.assertLess(source.index("await refreshHistory()"), source.index("delete nextStreams[jobId]"))
        self.assertIn("@keyframes chatStreamMessageIn", styles)
        self.assertIn("@keyframes chatStreamTextIn", styles)


if __name__ == "__main__":
    unittest.main()
