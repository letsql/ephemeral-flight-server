import cloudpickle
import pyarrow as pa
import pyarrow.flight as paf
import toolz


make_flight_result = toolz.compose(
    paf.Result,
    pa.py_buffer,
    cloudpickle.dumps,
)
