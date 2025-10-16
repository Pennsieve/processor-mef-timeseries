# Build JRE
FROM eclipse-temurin:17.0.10_7-jre-jammy AS jre

# Builder stage: clone and build edfwriter (produces jar)
FROM maven:3.9.4-eclipse-temurin-17 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /build
# Clone the edfwriter repo and build the project. We skip tests to speed the build.
RUN git clone https://github.com/Pennsieve/edfwriter . \
	&& mvn -B clean package -DskipTests

FROM python:3.12

# Copy JRE from the jre stage
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Copy the built jar from the builder stage
COPY --from=builder /build/target/mef2edf-0.0.1-SNAPSHOT-jar-with-dependencies.jar /processor/mefstreamer.jar

# Sanity test
RUN java -version && python --version

WORKDIR /processor

RUN apt clean && apt-get update && apt-get -y install libhdf5-dev

COPY processor/requirements.txt /processor/requirements.txt

RUN pip install -r /processor/requirements.txt

COPY processor/ /processor

ENV PYTHONPATH="/"

CMD ["python3.12", "-m", "processor.main"]
