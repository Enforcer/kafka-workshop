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


# After running the above function, you can call instrumenting:

# Instrumenting producer & consumer
# proxy_producer = ConfluentKafkaInstrumentor.instrument_producer(some_producer)
# proxy_consumer = ConfluentKafkaInstrumentor.instrument_consumer(some_consumer)

# Custom span
# from opentelemetry import trace
# tracer = trace.get_tracer(__name__)
# with tracer.start_as_current_span(name="custom name of custom span"):
#     ...
