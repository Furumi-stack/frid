import io
import json
import unittest
import urllib.error
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import publish_crates


class PublishCratesTests(unittest.TestCase):
    def test_publish_job_does_not_rebuild_verified_packages(self):
        with patch("sys.argv", ["publish_crates.py", "--no-verify"]):
            with patch.dict(publish_crates.os.environ, {"CARGO_REGISTRY_TOKEN": "test-only"}):
                with patch.object(publish_crates.subprocess, "check_output", return_value=json.dumps({})):
                    with patch.object(publish_crates, "unpublished_packages", return_value=["music-dht", "furumi-library"]):
                        with patch.object(publish_crates.subprocess, "run") as run:
                            publish_crates.main()
        run.assert_called_once_with([
            "cargo", "publish", "--registry", "crates-io", "--locked", "--all-features",
            "--package", "music-dht", "--package", "furumi-library", "--no-verify",
        ], check=True)

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
                with patch.object(publish_crates, "is_published", side_effect=lambda name, *args: name in published):
                    self.assertEqual(publish_crates.unpublished_packages(metadata), expected)

    def test_mirror_waits_for_workspace_dependencies_without_building(self):
        metadata = {"workspace_members": ["lib", "dht"], "packages": [
            {"id": "lib", "name": "furumi-library", "version": "0.3.0", "dependencies": [
                {"name": "music-dht", "path": "/workspace/music-dht", "kind": None},
            ]},
            {"id": "dht", "name": "music-dht", "version": "0.6.0", "dependencies": []},
        ]}
        with patch.object(publish_crates, "is_published", side_effect=[False, True]) as published:
            with patch.object(publish_crates.time, "sleep") as sleep, redirect_stdout(io.StringIO()):
                publish_crates.wait_for_mirror_dependencies(metadata, ["furumi-library"])
        self.assertEqual(published.call_count, 2)
        sleep.assert_called_once_with(10)

    def test_gitea_uses_its_own_index(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"vers":"0.3.1"}\n'
        with patch.object(publish_crates.urllib.request, "urlopen", return_value=response) as fetch:
            self.assertTrue(publish_crates.is_published("federation-net", "0.3.1", "gitea"))
        self.assertEqual(fetch.call_args.args[0].full_url,
                         "https://gt.hexor.cy/api/packages/Furumi/cargo/fe/de/federation-net")

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
