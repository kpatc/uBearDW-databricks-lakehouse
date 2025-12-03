FROM quay.io/debezium/server:2.5.4.Final

# Metadata
LABEL maintainer="uBear Data Team"
LABEL description="Debezium Server with Google Cloud Pub/Sub Sink"

# Set working directory
WORKDIR /debezium

# Copy configuration
COPY application.properties /debezium/conf/application.properties

# Copy service account key (will be mounted at runtime)
# Note: Key should be mounted as a secret in Cloud Run

# Create data directory for offsets
RUN mkdir -p /debezium/data && \
    chmod -R 755 /debezium/data

# Set environment variables (can be overridden at runtime)
ENV GOOGLE_APPLICATION_CREDENTIALS=/debezium/secrets/debezium-sa-key.json
ENV DEBEZIUM_OFFSET_STORAGE_FILE_FILENAME=/debezium/data/offsets.dat

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/q/health || exit 1

# Expose port (for health checks)
EXPOSE 8080

# Run Debezium Server
CMD ["/debezium/run.sh"]
