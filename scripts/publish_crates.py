"""Publish missing workspace versions together; Cargo orders their dependencies."""

import argparse
import json
import os
import subprocess
import urllib.error
import urllib.request


def index_path(name):
    name = name.lower()
    if len(name) < 3:
        return f"{len(name)}/{name}"
    if len(name) == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def is_published(name, version):
    request = urllib.request.Request(
        f"https://index.crates.io/{index_path(name)}",
        headers={"User-Agent": "frid-release-ci", "Cache-Control": "no-cache"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            versions = [json.loads(line) for line in response.read().splitlines()]
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise
    return any(row["vers"] == version for row in versions)


def unpublished_packages(metadata):
    members = set(metadata["workspace_members"])
    packages = []
    for package in sorted(metadata["packages"], key=lambda item: item["name"]):
        registries = package["publish"]
        if package["id"] not in members or (
            registries is not None and "crates-io" not in registries
        ):
            continue
        name, version = package["name"], package["version"]
        if is_published(name, version):
            print(f"Skip {name} {version}: already published", flush=True)
        else:
            print(f"Publish {name} {version}", flush=True)
            packages.append(name)
    return packages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="list packages without publishing")
    parser.add_argument("--dry-run", action="store_true", help="verify packages without uploading")
    args = parser.parse_args()
    if not (args.plan or args.dry_run) and not os.environ.get("CARGO_REGISTRY_TOKEN"):
        parser.error("CARGO_REGISTRY_TOKEN must be set for publishing")
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"],
        text=True,
    ))
    packages = unpublished_packages(metadata)
    if args.plan or not packages:
        return
    command = ["cargo", "publish", "--registry", "crates-io", "--locked", "--all-features"]
    for name in packages:
        command.extend(["--package", name])
    if args.dry_run:
        command.append("--dry-run")
    # Multi-package publishing verifies against a temporary local registry,
    # then uploads and waits for dependencies to appear before their users.
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
