FROM alpine:3.20

RUN apk add --no-cache python3 rsync curl tzdata su-exec

COPY engine.py /app/engine.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV PORT=49999 \
    STATE_DIR=/state \
    SRC_BASE=/mnt/quark \
    DST_BASE=/sync/quark \
    TZ=Asia/Shanghai

VOLUME ["/state"]
EXPOSE 49999

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${PORT}/" >/dev/null || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
