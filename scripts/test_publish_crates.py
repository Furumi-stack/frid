import io
import unittest
import urllib.error
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import publish_crates


class PublishCratesTests(unittest.TestCase):
    def test_existing_and_private_versions_are_excluded_on_retry(self):
        packages = [
            {"id": "net", "name": "federation-net", "version": "0.3.1", "publish": None},
            {"id": "dht", "name": "music-dht", "version": "0.6.0", "publish": None},
            {"id": "lib", "name": "furumi-library", "version": "0.3.0", "publish": None},
            {"id": "demo", "name": "demo", "version": "0.1.0", "publish": []},
            {"id": "internal", "name": "internal", "version": "0.1.0", "publish": ["private"]},
            {"id": "external", "name": "external", "version": "0.1.0", "publish": None},
        ]
        metadata = {"packages": packages, "workspace_members": [p["id"] for p in packages[:-1]]}
        for published, expected in [
            ({"federation-net"}, ["furumi-library", "music-dht"]),
            ({"federation-net", "music-dht"}, ["furumi-library"]),
        ]:
            with self.subTest(published=published), redirect_stdout(io.StringIO()):
                with patch.object(publish_crates, "is_published", side_effect=lambda name, _: name in published):
                    self.assertEqual(publish_crates.unpublished_packages(metadata), expected)

    def test_registry_errors_do_not_look_like_unpublished_versions(self):
        for status in (403, 429, 500):
            error = urllib.error.HTTPError("https://index.crates.io/test", status, "error", {}, None)
            with self.subTest(status=status), patch.object(publish_crates.urllib.request, "urlopen", side_effect=error):
                with self.assertRaises(urllib.error.HTTPError):
                    publish_crates.is_published("music-dht", "0.6.0")

    def test_missing_crate_and_existing_version(self):
        error = urllib.error.HTTPError("https://index.crates.io/test", 404, "missing", {}, None)
        with patch.object(publish_crates.urllib.request, "urlopen", side_effect=error):
            self.assertFalse(publish_crates.is_published("new-crate", "0.1.0"))
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"vers":"0.6.0","yanked":false}\n'
        with patch.object(publish_crates.urllib.request, "urlopen", return_value=response):
            self.assertTrue(publish_crates.is_published("music-dht", "0.6.0"))
            self.assertFalse(publish_crates.is_published("music-dht", "0.7.0"))


if __name__ == "__main__":
    unittest.main()
