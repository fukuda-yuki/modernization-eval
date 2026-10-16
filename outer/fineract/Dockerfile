# Resolved official inputs. Preparation records the resulting platform image ID.
FROM eclipse-temurin:21-jdk-noble@sha256:b468c3fc688b14450571494f588bd939378e7fd542ed5a73f8efc13f17872a87 AS java
RUN apt-get update && apt-get install -y --no-install-recommends python3 git curl ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*
COPY outer/fineract/limits.gradle /opt/fineract/limits.gradle
COPY outer/fineract/public-sdk.gradle /opt/fineract/public-sdk.gradle
COPY --chmod=0555 outer/fineract/gradle_run.py /usr/local/bin/fineract-gradle
ENV GRADLE_USER_HOME=/state/gradle HOME=/state/home PYTHONDONTWRITEBYTECODE=1
WORKDIR /workspace/source

FROM java AS worker
RUN curl -fsSL --retry 3 https://github.com/anomalyco/opencode/releases/download/v1.17.11/opencode-linux-x64.tar.gz -o /tmp/opencode.tar.gz \
    && echo '6aefcbbb7f04cdb4642be5208ddbfabb3c3d274f816bf42bff72eea5c244dca2  /tmp/opencode.tar.gz' | sha256sum -c - \
    && tar -xzf /tmp/opencode.tar.gz -C /usr/local/bin && rm /tmp/opencode.tar.gz \
    && test "$(opencode --version)" = 1.17.11
ENV XDG_CONFIG_HOME=/state/config XDG_DATA_HOME=/state/data XDG_CACHE_HOME=/state/cache \
    OPENCODE_DISABLE_AUTOUPDATE=true OPENCODE_DISABLE_MODELS_FETCH=true \
    OPENCODE_DISABLE_DEFAULT_PLUGINS=true
USER 1000:1000
WORKDIR /workspace

FROM python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258 AS gateway
COPY --chmod=0555 outer/harness/gateway.py outer/harness/campaign_budget.py /app/
COPY --chmod=0555 outer/fineract/probe_gateway.py /app/probe_gateway.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1000
ENTRYPOINT ["python3", "/app/gateway.py"]

# Controller regression tests include the retained Node-based observer tests.
# This stage is never the model worker or the business evaluator.
FROM java AS controller-tests
RUN apt-get update && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /repo

FROM java AS api-tests
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*
USER 1000:1000
WORKDIR /tmp
