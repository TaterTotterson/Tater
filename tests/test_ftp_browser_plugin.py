import unittest
from unittest.mock import patch

from plugins.ftp_browser import FtpBrowserPlugin


class FtpBrowserPluginTests(unittest.TestCase):
    def test_normalize_path(self):
        self.assertEqual(FtpBrowserPlugin._normalize_path(""), "/")
        self.assertEqual(FtpBrowserPlugin._normalize_path("movies/new"), "/movies/new")
        self.assertEqual(FtpBrowserPlugin._normalize_path("/movies//new/../"), "/movies")
        self.assertEqual(FtpBrowserPlugin._normalize_path("\\music\\mixes"), "/music/mixes")

    def test_join_path_uses_safe_basename(self):
        self.assertEqual(FtpBrowserPlugin._join_path("/media", "clips/trailer.mp4"), "/media/trailer.mp4")
        self.assertEqual(FtpBrowserPlugin._join_path("/media", "../secrets.txt"), "/media/secrets.txt")
        self.assertEqual(FtpBrowserPlugin._join_path("/", "folder"), "/folder")

    def test_coerce_int_clamps_range(self):
        self.assertEqual(FtpBrowserPlugin._coerce_int("70000", default=21, minimum=1, maximum=65535), 65535)
        self.assertEqual(FtpBrowserPlugin._coerce_int("-2", default=21, minimum=1, maximum=65535), 1)
        self.assertEqual(FtpBrowserPlugin._coerce_int("abc", default=21, minimum=1, maximum=65535), 21)

    def test_decode_map_handles_bytes(self):
        decoded = FtpBrowserPlugin._decode_map({b"FTP_PORT": b"21", "FTP_HOST": "ftp.local", b"FTP_USER": None})
        self.assertEqual(decoded.get("FTP_PORT"), "21")
        self.assertEqual(decoded.get("FTP_HOST"), "ftp.local")
        self.assertEqual(decoded.get("FTP_USER"), "")

    def test_get_ftp_conn_context_uses_clamped_port(self):
        settings = {"FTP_HOST": "ftp.local", "FTP_PORT": "70000", "FTP_USER": "alice", "FTP_PASS": "secret"}
        with patch.object(FtpBrowserPlugin, "_load_settings", return_value=settings), patch(
            "plugins.ftp_browser.aioftp.Client.context", return_value="ctx"
        ) as context_mock:
            context = FtpBrowserPlugin.get_ftp_conn_context()

        self.assertEqual(context, "ctx")
        self.assertEqual(context_mock.call_args.kwargs.get("port"), 65535)
        self.assertEqual(context_mock.call_args.kwargs.get("user"), "alice")

    def test_get_ftp_conn_context_requires_host(self):
        with patch.object(FtpBrowserPlugin, "_load_settings", return_value={"FTP_HOST": ""}):
            with self.assertRaises(ValueError):
                FtpBrowserPlugin.get_ftp_conn_context()

    def test_safe_label_truncates_long_value(self):
        label = FtpBrowserPlugin.safe_label("a" * 120, is_dir=False)
        self.assertTrue(label.endswith("..."))
        self.assertLessEqual(len(label), 80)


if __name__ == "__main__":
    unittest.main()
