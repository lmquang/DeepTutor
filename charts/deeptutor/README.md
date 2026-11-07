# DeepTutor on k3s

This chart runs the existing production Docker image as one non-root pod with
one persistent data volume. `Recreate` avoids concurrent writers to SQLite.
Backend port 8001 is internal; frontend port 3782 proxies API and WebSocket
requests on the same origin. No external database or Redis is needed with
the default single backend worker.

`deploy/k3s-values.yaml` targets `quang-pc`, uses a 20 GiB `local-path` PVC,
and enables private Tailscale HTTPS ingress. No public ingress is created.
It references the namespace-local `registry-credentials` image pull Secret
(copied from `hedge` for this cluster); credentials are not stored in values.
Tailscale uses `spec.tls.hosts` for the requested device name; see the
[operator documentation](https://tailscale.com/docs/kubernetes-operator/ingress/expose-workload-to-tailnet-l7).

## Indexing isolation

`indexing.backend` defaults to `subprocess`. Set it to `kubernetes` to run API
uploads, linked-folder sync, initial indexing, and reindex in short-lived Jobs.
The API then receives a narrowly scoped service-account token which can only
create, get, and delete `batch/jobs` in its namespace. Worker pods do not mount
a token. The full Job JSON is mounted at `/etc/deeptutor/indexing-job.json`.
Jobs use the release image, pull secrets, data PVC, GitHub Secret environment,
workspace roots and timezone; they have `backoffLimit: 0` and dedicated
resources (`indexing.resources`, default 1 CPU/1 Gi request and 2 CPU/4 Gi
limit). `indexing.maxIsolatedWorkers` defaults to one.

`indexing.timeoutSeconds` defaults to 21600 (six hours). With the subprocess
backend, children share the API pod's limits. With Kubernetes Jobs, required
same-node pod affinity keeps each worker beside the API because a local-path
RWO volume cannot be mounted across nodes. This provides CPU/memory isolation,
not disk or trust isolation: API and worker share the PVC, including private
directories and trusted pickle data. Ensure the API node has capacity for both.
Running Jobs are not managed by a newly rolled-out API revision and may retain
the previous image/template until completion; coordinate upgrades accordingly.
Failed-run IPC request files under the indexing job root may contain sensitive
inputs, including repository credentials or keys. Clean them up manually only
after confirming the corresponding worker is no longer running.
Keep one backend worker and one replica.

An optional `githubSecret` references an existing Secret with the key
`GITHUB_TOKEN`; no token value belongs in Helm values. The cluster override
references `deeptutor-github`, preserving authentication on future upgrades.

## Build

Run from the checkout root. The existing Dockerfile bundles Web/API and includes
the local Vietnamese changes. Runtime data and `.env` are excluded from the
build context. Use a new immutable image tag for each subsequent build.
The frontend builds for the target architecture because its standalone bundle
contains native image libraries; the Dockerfile verifies `sharp` before export.

```sh
docker buildx build --platform linux/amd64 --target production \
  -t deeptutor:vi-20261028 --load .
docker tag deeptutor:vi-20261028 registry.chartify.xyz/deeptutor:vi-20261028
docker push registry.chartify.xyz/deeptutor:vi-20261028
```

## Configure and install

Non-secret system/interface defaults are rendered into a ConfigMap from
`config.system` and `config.interface` in Helm values. The credential-bearing
model catalog Secret is created separately so API keys never enter Helm
values, chart archives, or the image. These commands seed only settings,
not local conversations or knowledge bases. Review provider URLs and ports
before using settings from another installation; this chart expects 8001/3782
and one backend worker.

```sh
kubectl --context k3s create namespace deeptutor
kubectl --context k3s -n deeptutor create secret generic deeptutor-bootstrap \
  --from-file=model_catalog.json=data/user/settings/model_catalog.json
helm lint charts/deeptutor -f deploy/k3s-values.yaml
helm --kube-context k3s upgrade --install deeptutor charts/deeptutor \
  --namespace deeptutor -f deploy/k3s-values.yaml --wait --timeout 10m
kubectl --context k3s -n deeptutor get pods,pvc,ingress -o wide
```

The init container copies only missing settings files from the ConfigMap and
Secret. Subsequent restarts and upgrades preserve edits made through the Web
app. Change existing settings in the app; updating bootstrap resources does
not overwrite the PVC's settings.
The PVC is retained by Helm on uninstall. Back up the entire `/app/data` tree;
local-path data stays on `quang-pc` and is not replicated across nodes.

Access the HTTPS hostname reported in the Ingress status from your tailnet.
Keep this ingress private when app login is disabled; access follows tailnet
policy. The chart does not enable Tailscale Funnel.

```sh
kubectl --context k3s -n deeptutor exec deploy/deeptutor -- \
  python -m deeptutor_cli.main doctor --online --format json
helm --kube-context k3s -n deeptutor history deeptutor
# Roll back an image/chart revision while retaining the data volume:
helm --kube-context k3s -n deeptutor rollback deeptutor REVISION --wait
```
