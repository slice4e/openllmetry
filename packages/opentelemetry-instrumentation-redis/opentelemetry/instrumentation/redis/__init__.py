"""OpenTelemetry Redis instrumentation"""

import logging
from opentelemetry.instrumentation.redis.config import Config
from opentelemetry.instrumentation.redis.utils import dont_throw
import redis
from typing import Collection
from wrapt import wrap_function_wrapper

from opentelemetry import context as context_api
from opentelemetry.trace import get_tracer, SpanKind
from opentelemetry.trace.status import Status, StatusCode

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import (
    _SUPPRESS_INSTRUMENTATION_KEY,
    unwrap,
)
from opentelemetry.semconv.ai import EventAttributes, Events
from opentelemetry.instrumentation.redis.version import __version__

#from opentelemetry.semconv.ai import SpanAttributes
from opentelemetry.semconv.trace import SpanAttributes

logger = logging.getLogger(__name__)

_instruments = ("redis-client >= 1.0.1",)


WRAPPED_METHODS = [
    {
        "package": redis,
        "object": "Redis",
        "method": "ping",
        "span_name": "ping",
    },
]


def _set_span_attribute(span, name, value):
    if value is not None:
        if value != "":
            span.set_attribute(name, value)
    return

@dont_throw
def _set_generic_span_attributes(span):
    print("In _set_generic_span_attributes\n")
    _set_span_attribute(span, "redis.generic", "generic")


def _with_tracer_wrapper(func):
    """Helper for providing tracer for wrapper functions."""

    def _with_tracer(tracer, to_wrap):
        def wrapper(wrapped, instance, args, kwargs):
            return func(tracer, to_wrap, wrapped, instance, args, kwargs)

        return wrapper

    return _with_tracer


@_with_tracer_wrapper
def _wrap(tracer, to_wrap, wrapped, instance, args, kwargs):
    """Instruments and calls every function defined in TO_WRAP."""
    print("In _wrap\n")
    if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
        return wrapped(*args, **kwargs)

    name = to_wrap.get("span_name")
    print("span name: \n", name)
    with tracer.start_as_current_span(name) as span:
        
        span.set_attribute(SpanAttributes.DB_SYSTEM, "redis")
        span.set_attribute(SpanAttributes.DB_OPERATION, to_wrap.get("method"))
        
        response = wrapped(*args, **kwargs)

        _set_generic_span_attributes(span)
        return response


class RedisInstrumentor(BaseInstrumentor):
    """An instrumentor for Redis's client library."""

    def __init__(self, exception_logger=None):
        super().__init__()
        Config.exception_logger = exception_logger
        print("In instrumentation_redis __init__\n")

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        print("In instrumentation_redis _instrument\n")
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(__name__, __version__, tracer_provider)
        for wrapped_method in WRAPPED_METHODS:
            wrap_package = wrapped_method.get("package")
            wrap_object = wrapped_method.get("object")
            wrap_method = wrapped_method.get("method")
            if getattr(wrap_package, wrap_object, None):
                wrap_function_wrapper(
                    wrap_package,
                    f"{wrap_object}.{wrap_method}",
                    _wrap(tracer, wrapped_method),
                )

    def _uninstrument(self, **kwargs):
        print("In instrumentation_redis _uninstrument\n")
        for wrapped_method in WRAPPED_METHODS:
            wrap_package = wrapped_method.get("package")
            wrap_object = wrapped_method.get("object")
            
            wrapped = getattr(wrap_package, wrap_object, None)
            if wrapped:
                unwrap(wrapped, wrapped_method.get("method"))
