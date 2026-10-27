FROM docker.io/otel/opentelemetry-collector-contrib:0.163.2-386
COPY docker/otel-collector/config.yml /etc/otelcol-contrib/config.yml