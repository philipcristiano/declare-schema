FROM rust:1.88-bookworm as builder
WORKDIR /usr/src/app

COPY . .
RUN SQLX_OFFLINE=true cargo install --path .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y procps ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/declare-schema /usr/local/bin/declare-schema


ENTRYPOINT ["/usr/local/bin/declare-schema"]
