# Google Cloud Log Forwarder for TrueWatch

A small Cloud Run service that forwards Google Cloud Logging entries to TrueWatch through a DataKit
inside your VPC. It is one Python file that uses only the standard library.

```text
Cloud Logging sink -> Pub/Sub topic -> authenticated push (OIDC) -> Cloud Run (this service)
  -> private DataKit :9529 -> TrueWatch
```

## How it works

- A Log Router sink publishes the entries you select to a Pub/Sub topic. A push subscription with
  authentication enabled calls this service. Cloud Run IAM checks the caller, so deploy with
  `--no-allow-unauthenticated`; the service has no token verifier of its own.
- Each entry becomes one DataKit log record, posted to `http://<private IPv4>:9529/v1/write/logging`.
  The service acknowledges only after DataKit returns 2xx. Otherwise Pub/Sub retries, so a record can
  arrive twice; `google_insert_id` identifies duplicates.
- `jsonPayload` and `textPayload` are supported. Audit logs use `protoPayload` and are rejected.
- The TrueWatch DataWay token stays in DataKit. Neither this repository nor `env.yaml` holds a
  credential, and the service refuses to start if a credential-shaped variable is set.

## Get the code

```bash
git clone https://github.com/TrueWatchTech/google-cloud-log-forwarder.git
cd google-cloud-log-forwarder
```

## Configure

Edit `env.yaml`. It holds placeholders only:

```yaml
DATAKIT_URL: "http://DATAKIT_IPV4:9529/v1/write/logging"
EXPECTED_PROJECT_ID: "PROJECT_ID"
EXPECTED_SUBSCRIPTION: "projects/PROJECT_ID/subscriptions/SUBSCRIPTION_ID"
# MEASUREMENT: "gcp_pubsub"  # optional DataKit log source name
```

`DATAKIT_URL` must use a private IPv4 address in 10/8, 172.16/12 or 192.168/16, port 9529 and the
path shown, with no credentials or query string.

## Deploy

Cloud Build builds the image, so its build service account needs **Cloud Run Builder**
(`roles/run.builder`) and you need the permissions in
[Deploy services from source code](https://docs.cloud.google.com/run/docs/deploying-source-code).
From this directory:

```bash
gcloud run deploy pubsub-log-forwarder \
  --source . \
  --project PROJECT_ID \
  --region REGION \
  --no-allow-unauthenticated \
  --service-account forwarder-run@PROJECT_ID.iam.gserviceaccount.com \
  --network VPC_NETWORK \
  --subnet SUBNET \
  --vpc-egress private-ranges-only \
  --cpu 1 --memory 512Mi \
  --min-instances 0 --max-instances 1 --concurrency 1 \
  --env-vars-file env.yaml
```

`.gcloudignore` uploads only `main.py` and `Dockerfile`. The sizing flags are starting values, not a
measured minimum. Then grant your push service account **Cloud Run Invoker** on this service and
create the push subscription with endpoint `https://SERVICE_URL/pubsub`, audience
`https://SERVICE_URL` and payload unwrapping off. See
[push authentication](https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions) and
[Direct VPC egress](https://docs.cloud.google.com/run/docs/configuring/vpc-direct-vpc).

A step-by-step TrueWatch guide will be published at
<https://docs.truewatch.com/integrations/google_pubsub_http_endpoint/>; that page is not live yet.

## Test

```bash
python3 tests/test_forwarder.py
```

The tests need Python 3 only. They run the real HTTP handler against a loopback stub, which proves
this service's behaviour, not delivery to TrueWatch.

## Security and license

See [SECURITY.md](SECURITY.md). Licensed under the Apache License 2.0; see [LICENSE](LICENSE).
