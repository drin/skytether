"""
Initialization fro skytether package.
"""

#import gc

# This is the package version
__version__ = '0.1.0'

# ARROW-8684: Disable GC while initializing Cython extension module,
# to workaround Cython bug in https://github.com/cython/cython/issues/3603
# _gc_enabled = gc.isenabled()
# gc.disable()
# import skytether.lib as _lib
# if _gc_enabled:
#     gc.enable()
