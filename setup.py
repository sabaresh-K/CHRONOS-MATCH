"""
ChronosMatch Cython C-Extension Build Configuration
Compiles native matching engine with maximum optimization and generates HTML C-API annotations.
"""

import sys
from setuptools import setup, Extension

try:
    from Cython.Build import cythonize
    HAVE_CYTHON = True
except ImportError:
    HAVE_CYTHON = False

# High-performance compiler and link flags
if sys.platform == "win32":
    extra_compile_args = ["/O2", "/Oi", "/Ot", "/GL"]
    extra_link_args = ["/LTCG"]
else:
    extra_compile_args = ["-O3", "-march=native", "-ffast-math", "-flto"]
    extra_link_args = ["-flto"]

ext_modules = []
if HAVE_CYTHON:
    ext_modules = cythonize(
        [
            Extension(
                "orderbook",
                sources=["orderbook.pyx"],
                extra_compile_args=extra_compile_args,
                extra_link_args=extra_link_args,
            ),
            Extension(
                "core.lob_engine",
                sources=["core/lob_engine.pyx"],
                extra_compile_args=extra_compile_args,
                extra_link_args=extra_link_args,
            ),
        ],
        annotate=True,  # Generates core/lob_engine.html for C-API overhead inspection
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "nonecheck": False,
            "initializedcheck": False,
        },
    )

setup(
    name="chronosmatch",
    version="1.0.0",
    description="ChronosMatch: Zero-Copy High-Frequency Trading Engine",
    author="ChronosMatch Architect",
    ext_modules=ext_modules,
    packages=["core", "services", "tests"],
    python_requires=">=3.10",
)
