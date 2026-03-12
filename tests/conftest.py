"""Global test configuration.

Disables dask's automatic pyarrow string conversion so that object-dtype
string columns behave like standard pandas strings (e.g., str.startswith
accepts tuples). This matches the behavior expected by the codebase, which
was written before dask adopted pyarrow-backed strings by default.
"""
import dask

dask.config.set({"dataframe.convert-string": False})
