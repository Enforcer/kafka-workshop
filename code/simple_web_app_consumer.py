from confluent_kafka import Consumer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

TOPIC = "web-app-producer"
config = {
    "bootstrap.servers": "broker:9092",
    "group.id": "web-app-consumer",
    "auto.offset.reset": "earliest",
    "group.instance.id": "web-app-consumer-singleton",
}


from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.zipkin.proto.http import ZipkinExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from sqlalchemy import Engine


def setup_tracer(
    service_name: str, app: FastAPI | None = None, engine: Engine | None = None
) -> None:
    resource = Resource(attributes={SERVICE_NAME: service_name})
    zipkin_exporter = ZipkinExporter(endpoint=f"http://zipkin:9411/api/v2/spans")
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(zipkin_exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)

    RequestsInstrumentor().instrument()
    if engine is not None:
        SQLAlchemyInstrumentor().instrument(
            engine=engine, enable_commenter=True, commenter_options={}
        )
    else:
        SQLAlchemyInstrumentor().instrument(enable_commenter=True, commenter_options={})

    if app is not None:
        FastAPIInstrumentor().instrument_app(app)

    ConfluentKafkaInstrumentor().instrument()


def main() -> None:
    tracer = trace.get_tracer(__name__)
    propagator = TraceContextTextMapPropagator()
    consumer = ConfluentKafkaInstrumentor.instrument_consumer(Consumer(config))
    consumer.subscribe([TOPIC])

    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue

        headers = {k: v.decode() for k, v in message.headers()}
        context = propagator.extract(carrier=headers)
        with tracer.start_as_current_span("Handling message", context=context):
            print(f"Consumed message: {message.value().decode('utf-8')}")
            consumer.commit(message)


if __name__ == "__main__":
    setup_tracer("web-app-consumer")
    main()
