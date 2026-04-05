import contextlib
import io
import warnings

warnings.filterwarnings("ignore", message=".*kagglehub.*", category=UserWarning)
warnings.filterwarnings("ignore", message=".*outdated.*", category=UserWarning)

# kagglehub prints its version-check warning directly to stdout at import time.
# Pre-import it here with output suppressed so sys.modules caches it silently;
# subsequent imports by perch_hoplite hit the cache and produce no output.
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    try:
        import kagglehub  # noqa: F401
    except Exception:
        pass
