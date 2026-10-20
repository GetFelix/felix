FROM docker.io/otel/opentelemetry-collector-contrib:0.163.1
COPY docker/otel-collector/config.yml /etc/otelcol-contrib/config.yml