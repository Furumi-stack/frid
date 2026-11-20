"""Publish missing workspace versions together; Cargo orders their dependencies."""

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

INDEXES = {
    "crates-io": "https://index.crates.io/",
    "gitea": "https://gt.hexor.cy/api/packages/Furumi/cargo/",
}


def index_path(name):
    name = name.lower()
    if len(name) < 3:
        return f"{len(name)}/{name}"
    if len(name) == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def is_published(name, version, registry="crates-io"):
    request = urllib.request.Request(
        INDEXES[registry] + index_path(name),
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


def unpublished_packages(metadata, registry="crates-io"):
    members = set(metadata["workspace_members"])
    packages = []
    for package in sorted(metadata["packages"], key=lambda item: item["name"]):
        registries = package["publish"]
        if package["id"] not in members or (
            registries is not None and registry not in registries
        ):
            continue
        name, version = package["name"], package["version"]
        if is_published(name, version, registry):
            print(f"Skip {name} {version}: already published", flush=True)
        else:
            print(f"Publish {name} {version}", flush=True)
            packages.append(name)
    return packages


def wait_for_mirror_dependencies(metadata, selected):
    # Mirrored manifests retain their crates.io dependencies. In the parallel
    # upload job, wait for new workspace dependencies to reach crates.io before
    # Cargo resolves the mirror's packages (it does not compile them again).
    members = {p["name"]: p for p in metadata["packages"] if p["id"] in metadata["workspace_members"]}
    required = {
        (dep["name"], members[dep["name"]]["version"])
        for name in selected
        for dep in members[name]["dependencies"]
        if dep.get("path") and dep["name"] in members and dep["kind"] != "dev"
    }
    deadline = time.monotonic() + 600
    while required:
        required = {(name, version) for name, version in required if not is_published(name, version)}
        if not required:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Workspace dependencies did not reach crates.io: {sorted(required)}")
        print(f"Waiting for crates.io dependencies before mirroring: {sorted(required)}", flush=True)
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="list packages without publishing")
    parser.add_argument("--dry-run", action="store_true", help="verify packages without uploading")
    parser.add_argument("--registry", choices=INDEXES, default="crates-io")
    parser.add_argument("--no-verify", action="store_true", help="publish packages already verified by CI without rebuilding")
    args = parser.parse_args()
    token_variable = "CARGO_REGISTRY_TOKEN" if args.registry == "crates-io" else "CARGO_REGISTRIES_GITEA_TOKEN"
    if not (args.plan or args.dry_run) and not os.environ.get(token_variable):
        parser.error(f"{token_variable} must be set for publishing")
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"],
        text=True,
    ))
    packages = unpublished_packages(metadata, args.registry)
    if args.plan or not packages:
        return
    if args.registry == "gitea":
        wait_for_mirror_dependencies(metadata, packages)
    command = ["cargo", "publish", "--registry", args.registry, "--locked", "--all-features"]
    for name in packages:
        command.extend(["--package", name])
    if args.dry_run:
        command.append("--dry-run")
    if args.no_verify:
        command.append("--no-verify")
    # Multi-package publishing verifies against a temporary local registry,
    # then uploads and waits for dependencies to appear before their users.
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
