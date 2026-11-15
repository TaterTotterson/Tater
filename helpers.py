"""Compatibility import for :mod:`tater.llm.runtime`. Implementation lives there."""
if __name__ == "__main__":
    import os
    import sys
    from tater.llm import runtime
    if runtime._LLAMA_CPP_ENGINE_WORKER_ARG in sys.argv[1:]:
        runtime._llama_cpp_engine_worker_main()
        os._exit(1)
    if runtime._LLAMA_CPP_VISION_WORKER_ARG in sys.argv[1:]:
        runtime._llama_cpp_vision_worker_main()
        os._exit(1)

from tater._compat import redirect_module as _redirect_module
_redirect_module(__name__, "tater.llm.runtime")
