# Security

## Reporting a vulnerability

If private vulnerability reporting is enabled for this repository, report through the
**Security** tab with **Report a vulnerability**. If it is not enabled, do not put secrets, DataWay
URLs, tokens, log contents or exploit details in a public issue; open an issue that only asks for a
private way to share the report.

## Deployment notes

- Deploy with `--no-allow-unauthenticated`. Cloud Run IAM is the only authentication; the project
  and subscription checks in the service filter foreign messages but do not authenticate callers.
- Keep the DataWay URL and token in DataKit. Never add them to `env.yaml`, environment variables or
  this repository.
- Keep DataKit's listener private: allow TCP 9529 only from the Cloud Run subnet.
