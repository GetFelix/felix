FROM docker.io/otel/opentelemetry-collector-contrib:0.162.0
COPY docker/otel-collector/config.yml /etc/otelcol-contrib/config.yml