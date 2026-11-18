# Google Cloud Log Forwarder for TrueWatch — DataWay edition

A small Cloud Run service that forwards Google Cloud Logging entries straight to TrueWatch DataWay,
with no DataKit in between. It is one Python file that uses only the standard library. The DataKit
edition in the repository root is unchanged; use this one when you do not run a DataKit in the VPC.

Run every command below from this `dataway/` directory.

```text
Cloud Logging sink -> Pub/Sub topic -> authenticated push (OIDC) -> Cloud Run (this service)
  -> HTTPS DataWay /v1/write/logging?token=... -> TrueWatch
```

## How it works

- A Log Router sink publishes the entries you select to a Pub/Sub topic. A push subscription with
  authentication enabled calls this service. Cloud Run IAM checks the caller, so deploy with
  `--no-allow-unauthenticated`.
- Each entry becomes one line-protocol point. Concurrent push requests are batched: up to
  `BATCH_MAX_RECORDS` points or `BATCH_MAX_BYTES` bytes, or whatever arrived within
  `BATCH_MAX_WAIT_MS`, go to DataWay in one POST over a kept-alive HTTPS connection, from `SENDERS`
  sender threads.
- A push request is acknowledged (204) only after the POST that carried its record got 2xx from
  DataWay. A failed POST answers 502 to every request in that batch; a full queue answers 503 at
  once. Pub/Sub redelivers both, so a record can arrive twice; `google_insert_id` identifies
  duplicates.
- Batching needs concurrent requests: deploy with `--concurrency` well above 1.
- `jsonPayload` and `textPayload` are supported. Audit logs use `protoPayload` and are rejected.
- `httpRequest` becomes `http_*` fields (`http_method`, `http_url`, `http_status`,
  `http_request_size`, `http_response_size`, `http_user_agent`, `http_remote_ip`, `http_server_ip`,
  `http_referer`, `http_protocol`, `http_latency`, `http_latency_ms`, cache fields).
- `resource.labels` string values become `resource_<key>` tags. Empty values are omitted.
- `EXTRA_TAGS` adds static tags such as `host=...`, which DataKit used to add as global tags.
- Pipelines configured on a DataKit do not run on this path. Central Pipelines do.

## Configure

The service accepts only a TrueWatch **Client Token** (create one under Management → Client Token).
A Client Token can write logs and RUM but cannot query or write other data types, so a leaked token
cannot read the workspace. A workspace `tkn_` token is refused at start. The service adds the
`to_headless=true` parameter a Client Token requires.

`env.yaml` holds placeholders only. The Client Token goes to Secret Manager:

```bash
gcloud services enable secretmanager.googleapis.com --project PROJECT_ID
printf '%s' 'CLIENT_TOKEN' | gcloud secrets create truewatch-dataway-token --data-file=- --project PROJECT_ID
gcloud secrets add-iam-policy-binding truewatch-dataway-token --project PROJECT_ID \
  --member serviceAccount:forwarder-run@PROJECT_ID.iam.gserviceaccount.com \
  --role roles/secretmanager.secretAccessor
```

`DATAWAY_URL` must be `https://` with a DNS name and no query string. The token is sent only as the
`token` query parameter and never logged. Any other credential-shaped environment variable still
refuses start.

## Deploy

```bash
gcloud run deploy pubsub-log-forwarder \
  --source . \
  --project PROJECT_ID \
  --region REGION \
  --no-allow-unauthenticated \
  --service-account forwarder-run@PROJECT_ID.iam.gserviceaccount.com \
  --set-secrets DATAWAY_TOKEN=truewatch-dataway-token:latest \
  --cpu 1 --memory 512Mi \
  --concurrency 250 --min-instances 1 --max-instances 10 \
  --env-vars-file env.yaml
```

The service needs internet egress to DataWay; it does not need Direct VPC egress. The sizing flags
are starting values, not a measured result.

Set the push subscription's ack deadline above the longest wait a request can have (batch window +
one POST with one retry, about 35 s):

```bash
gcloud pubsub subscriptions update SUBSCRIPTION_ID --project PROJECT_ID --ack-deadline 60
```

## Logs

One JSON line per push request (`outcome`, `google_insert_id`, `batch_records`, `http_status`) and
one per batch (`event: batch`, `records`, `bytes`, `dataway_status`, `post_ms`, `queued`). No
payload, response body or token is ever logged.

## Test

```bash
python3 tests/test_forwarder.py
python3 tests/test_lb.py
```

The tests run the real HTTP handler against a loopback stub DataWay that decodes the line protocol
with its own parser. They prove this service's behaviour, not delivery to TrueWatch.

## Security and license

- The Client Token lives in Secret Manager and reaches the service only as `DATAWAY_TOKEN` through
  `--set-secrets`. Never put it in `env.yaml`, a plain environment variable or this repository.
- Grant `roles/secretmanager.secretAccessor` on that one secret to the Cloud Run service account
  only. If the token may have leaked, rotate it in TrueWatch and add a new secret version.

See also [SECURITY.md](../SECURITY.md). Licensed under the Apache License 2.0; see
[LICENSE](../LICENSE).
