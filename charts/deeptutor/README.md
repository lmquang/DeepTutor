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
