# SID-IT-712 fork of OpenNebula/storage-provider-opennebula

This fork exists to run **one fix** that is not yet upstream, and to propose it
upstream from the exact commit we run.

## The fix

Upstream's stuck-attachment reconciler (`pkg/csi/driver/attachment_reconciler.go`)
lists every VolumeAttachment in the cluster and checks only `pv.Spec.CSI != nil`.
A PV belonging to **any other CSI driver** passes, its volume handle is never among
OpenNebula's observed attachments, and the stale-VolumeAttachment loop deletes it.

On a cluster running this driver beside Longhorn, it deleted 1,214 of Longhorn's
VolumeAttachments at about 16 per minute and detached live volumes from running pods.

The fix indexes only this driver's PVs, compared against the driver instance's own
name, so the stale-VA, orphan-detach and multi-attach paths all skip other drivers.
A regression test fails without it.

## Branches

| Branch | What it is | Rule |
|---|---|---|
| `master` | upstream, untouched | Never commit here. Sync from `upstream/master`. |
| `fix/reconciler-driver-filter` | **only** the fix, one commit on upstream | Upstream-clean: no fork plumbing, no private references. This is what an upstream PR would come from. |
| `sid/release` | the fix **plus** this file and our workflows | Our deploy line. Images are built from here. Never merge it into the fix branch. |

## Images

`ghcr.io/sid-it-712/opennebula-csi:sid-v<upstream>-<n>` — for example
`sid-v0.2.0-1` is upstream `v0.2.0` plus the fix, build 1. Built by
`.github/workflows/sid-image.yaml` when a `sid-v*` tag is pushed on `sid/release`.

- **Never push a `v*.*.*` tag.** Upstream's `release-csi.yaml` fires on those; it
  would try to push to `ghcr.io/opennebula`, publish charts to our `gh-pages` and
  cut a release.
- The workflow **refuses to publish** unless the filter is present and its
  regression test passes. Our deployment tooling lets only images from this
  repository run the reconciler, so every image published here must carry the fix.

## Rebasing onto a new upstream release

1. `git fetch upstream --tags`, then rebase `fix/reconciler-driver-filter` onto the new tag.
2. Rebase `sid/release` onto the rebased fix branch.
3. Run the regression test, then tag `sid-v<new-upstream>-1`.
4. If upstream has merged an equivalent fix, retire this fork: pin upstream's image
   in our deployment tooling, and trust its repository to run the reconciler only
   after re-running the regression test against it.

## Chart

We use upstream's own chart (`helm/opennebula-csi`) unchanged. Its default image,
`nudevco/opennebula-csi:v0.5.15`, is a third-party build. Always set
`image.repository` and `image.tag`. Set the reconciler flag with `--set-string`: the
chart reads it through `| default true`, and a boolean `false` renders as `"true"`.
