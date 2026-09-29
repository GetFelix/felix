FROM prom/prometheus:v3.15.0

# Prometheus reads /etc/prometheus/prometheus.yml by default in this image’s entrypoint.
COPY docker/prometheus/prometheus.yml /etc/prometheus/prometheus.yml
# Optional rules
# COPY docker/prometheus/rules.yml /etc/prometheus/rules.yml

CMD ["--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus"]