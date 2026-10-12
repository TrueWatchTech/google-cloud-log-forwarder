# pubsub-log-forwarder v02 - Python standard library only, no third-party dependency to lock.
# The base image is pinned by digest so a rebuild from the same source is reproducible.
# Built by Cloud Build during `gcloud run deploy --source .`; no local Docker is required.
FROM docker.io/library/python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY main.py /app/main.py
# Mode is set explicitly while still root, so the image does not depend on the local file mode
# and the non-root runtime can read but never modify its own code.
RUN chown 0:0 /app /app/main.py && chmod 0555 /app && chmod 0444 /app/main.py
USER 10001:10001
CMD ["python", "/app/main.py"]
