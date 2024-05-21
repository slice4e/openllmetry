from opentelemetry.instrumentation.redis.utils import dont_throw
from opentelemetry.instrumentation.utils import (
    _SUPPRESS_INSTRUMENTATION_KEY,
)
#from opentelemetry.trace.status import Status, StatusCode
from opentelemetry import context as context_api
from redis.commands.search.query import Query


def _set_span_attribute(span, name, value):
    if value is not None:
        if value != "":
            span.set_attribute(name, value)
    return

@dont_throw
def _set_generic_span_attributes(span):
    _set_span_attribute(span, "redis.generic", 1)

# Assume that the first argument is the Query object and we extract the query string  
# This approach works when the query string is not encoded using an embedding model. 
# If the query string is encoded, we need to decode it to get the original query string.
@dont_throw
def _set_search_attributes(span, args):
    _set_span_attribute(
        span,
        "redis.commands.search.query",
        args[0].query_string(),
    )

@dont_throw
def _set_create_index_attributes(span, kwargs):
    _set_span_attribute(
        span,
        "create_index",
        kwargs.get("fields").__str__(),
    )
    _set_span_attribute(
        span,
        "create_index",
        kwargs.get("definition").__str__(),
    )

# docs is a list. We could print out each separately.   
@dont_throw
def _add_search_result_events(span, response):
    _set_span_attribute(
        span,
        "redis.commands.search.total",
        response.total
    )
    _set_span_attribute(
        span,
        "redis.commands.search.duration",
        response.duration
    )
    _set_span_attribute(
        span,
        "redis.commands.search.docs",
        response.docs.__str__()
    )

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

        if to_wrap.get("method") == "search":
            _set_search_attributes(span, args)
        elif to_wrap.get("method") == "create_index":
            _set_create_index_attributes(span, kwargs)
        else:
            _set_generic_span_attributes(span)
            
        response = wrapped(*args, **kwargs)
        
        if to_wrap.get("method") == "search":
            _add_search_result_events(span, response)
        
        return response