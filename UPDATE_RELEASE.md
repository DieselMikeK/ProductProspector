## Update Release Flow

### One-time setup

1. In GitHub repo settings, enable Actions and set workflow permissions to `Read and write`.
2. If `master` is branch-protected, allow GitHub Actions to push to it or the workflow will publish the exe but fail to update [app/docs/release.json](./app/docs/release.json).

### Normal release flow

1. Push the code you want released to `master`.
2. Change [VERSION](./VERSION) before that push if you want a new release number.
3. Run `.\publish_release.ps1` from the repo root.
4. Optionally pass notes with `.\publish_release.ps1 -Notes "What changed"`.
5. Optionally let the script bump the version for the release commit with `.\publish_release.ps1 -Version 1.0.1 -Notes "What changed"`.

That command updates [release_request.json](./release_request.json), commits it, and pushes it to `master`. The `.github/workflows/release.yml` workflow watches that file and then:

- builds `ProductProspectorUpdater.exe` and `ProductProspector.exe`
- creates GitHub release `v<version>`
- uploads `app\dev\dist\ProductProspector.exe`
- updates [app/docs/release.json](./app/docs/release.json) with the primary exe URL, SHA-256, notes, and publish time
- pushes the manifest commit so installed apps see the `Update` button

Normal pushes do not trigger client updates. There are two separate files involved:

- [release_request.json](./release_request.json): the release trigger file that tells GitHub Actions to publish a release
- [app/docs/release.json](./app/docs/release.json): the client-visible manifest that makes installed apps show the `Update` button

Clients only see the `Update` button when the remote manifest version in [app/docs/release.json](./app/docs/release.json) is newer than their local [VERSION](./VERSION).
