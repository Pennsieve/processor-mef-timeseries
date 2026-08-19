# Build JRE
FROM eclipse-temurin:17.0.10_7-jre-jammy AS jre

# Builder stage: clone and build edfwriter (produces jar)
FROM maven:3.9.4-eclipse-temurin-17 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /build

# Branch, tag, or commit SHA of Pennsieve/edfwriter to build against.
# Pin to a SHA for a reproducible build; the default tracks the branch.
ARG EDFWRITER_REF=main

# Bust the cache when the ref moves.
#
# Without this, the clone below is one RUN layer whose command string never
# changes, so Docker reuses the cached layer forever and the image keeps
# shipping a stale jar no matter what landed on edfwriter. ADD re-fetches this
# URL on every build and invalidates the cache only when the commit it names
# actually changes — so a moving branch is picked up, and an unchanged one
# still builds fast.
ADD https://api.github.com/repos/Pennsieve/edfwriter/commits/${EDFWRITER_REF} /build/edfwriter-ref.json

# Clone the edfwriter repo and build the project. We skip tests to speed the build.
RUN git clone https://github.com/Pennsieve/edfwriter src \
	&& git -C src checkout --detach "${EDFWRITER_REF}" \
	&& git -C src log -1 --format='edfwriter build ref: %H %s' \
	&& mvn -B -f src/pom.xml clean package -DskipTests \
	&& cp src/target/mef2edf-0.0.1-SNAPSHOT-jar-with-dependencies.jar /build/mefstreamer.jar

FROM python:3.12

# Copy JRE from the jre stage
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Copy the built jar from the builder stage
COPY --from=builder /build/mefstreamer.jar /processor/mefstreamer.jar

# Sanity test
RUN java -version && python --version

WORKDIR /processor

RUN apt clean && apt-get update && apt-get -y install libhdf5-dev

COPY processor/requirements.txt /processor/requirements.txt

RUN pip install -r /processor/requirements.txt

COPY processor/ /processor

# pynwb/platformdirs needs a writable cache dir even when the container
# runs as a non-root user (HOME may be unset/"/" in the compute env)
ENV XDG_CACHE_HOME=/tmp/.cache

ENV PYTHONPATH="/"

CMD ["python3.12", "-m", "processor.main"]
