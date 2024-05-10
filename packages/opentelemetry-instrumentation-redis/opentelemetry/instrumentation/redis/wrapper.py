from opentelemetry.instrumentation.redis.utils import dont_throw
from opentelemetry.instrumentation.utils import (
    _SUPPRESS_INSTRUMENTATION_KEY,
)
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry import context as context_api



def _set_span_attribute(span, name, value):
    if value is not None:
        if value != "":
            span.set_attribute(name, value)
    return

@dont_throw
def _set_generic_span_attributes(span):
    _set_span_attribute(span, "redis.ping", 1)


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
    if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
        return wrapped(*args, **kwargs)

    name = to_wrap.get("span_name")
    with tracer.start_as_current_span(name) as span:      
        response = wrapped(*args, **kwargs)
        
        if response:
            span.add_event("redis.ping")
        
        _set_generic_span_attributes(span)
        
        return response