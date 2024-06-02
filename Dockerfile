FROM rust:1.77-bookworm as builder
WORKDIR /usr/src/app

COPY . .
RUN cargo install --path .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y procps ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/ds-migrate /usr/local/bin/ds-migrate


ENTRYPOINT ["/usr/local/bin/ds-migrate"]
