# RunPod Virtual Kubelet

Virtual kubelet provider for running Kubernetes pods on RunPod GPU instances.

## Managed EndpointSlices

The provider can publish RunPod pods behind ordinary Kubernetes Services. A
Service opts in with annotations:

```yaml
runpod.io/managed-endpoints: "true"
runpod.io/pod-selector: "app=my-runpod-model"
```

The Service should not have `spec.selector`. The provider reads
`runpod.io/pod-selector`, finds matching RunPod pods, and creates
`EndpointSlice` objects with:

```yaml
metadata:
  labels:
    kubernetes.io/service-name: <service-name>
    endpointslice.kubernetes.io/managed-by: runpod-kubelet
```

`EndpointSlice.metadata.name` is intentionally opaque:

```text
rnpd-<24 hex chars>
```

The object name is only Kubernetes object identity for get/update/delete. It is
not used by kube-proxy, CoreDNS, or Service DNS. Service discovery uses the
`kubernetes.io/service-name` label, so the DNS name remains:

```text
<service>.<namespace>.svc.cluster.local
```

The hash includes both Service identity and Pod identity. This matters because
one RunPod pod may be selected by multiple Services. Those Services need
different EndpointSlices: each slice has a different `kubernetes.io/service-name`
label, and Service port names can differ.

The provider currently creates one EndpointSlice per `(Service, Pod)` pair.
EndpointSlice ports are slice-wide, while RunPod external port mappings are
pod-specific, so grouping multiple RunPod pods into one slice would require all
of them to share identical port mappings.
