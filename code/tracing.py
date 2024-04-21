from opentelemetry import trace
from opentelemetry.exporter.zipkin.proto.http import ZipkinExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from sqlalchemy import Engine


def setup_tracer(
    service_name: str, engine: Engine | None = None
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

    ConfluentKafkaInstrumentor().instrument()


# Instrumenting producer & consumer
# some_producer = ConfluentKafkaInstrumentor.instrument_producer(some_producer)
# some_consumer = ConfluentKafkaInstrumentor.instrument_consumer(some_consumer)

# with tracer.start_as_current_span(name="custom name of custom span"):
#     ...
