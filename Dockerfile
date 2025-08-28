# Build JRE
FROM eclipse-temurin:17.0.10_7-jre-jammy AS jre

FROM python:3.12

# Copy JRE from the previous stage
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

# sanity test
RUN java -version && python --version

WORKDIR /processor

RUN apt clean && apt-get update && apt-get -y install libhdf5-dev

COPY processor/requirements.txt /processor/requirements.txt

COPY mefstreamer.jar /processor/mefstreamer.jar

RUN pip install -r /processor/requirements.txt

COPY processor/ /processor

ENV PYTHONPATH="/"

CMD ["python3.12", "-m", "processor.main"]
