#!/usr/bin/env python
"""
Build script for the skytether module. This includes how to compile cython bindings for skyhook.
"""

# ------------------------------
# Dependencies

# >> Modules
import os

# >> Third-party
import numpy
import pyarrow

# >> Classes
from setuptools import Extension

# >> Functions
from setuptools import setup
from Cython.Build import cythonize


# ------------------------------
# Module variables

# >> reference variables
kinetic_installdir   = os.path.join('/opt', 'kinetic')
skytether_installdir = os.path.join('/opt', 'skytether-singlecell')

cpp_srcdirpath = os.path.join('src', 'cpp')
py_srcdirpath  = os.path.join('src', 'python')
pkg_srcdirpath = os.path.join(py_srcdirpath, 'skytether', 'cybindings')

scytether_srclist = [
     os.path.join(pkg_srcdirpath, 'scytether.pyx')
    # ,os.path.join(pkg_srcdirpath, 'cykinetic.pyx')
    ,os.path.join(cpp_srcdirpath, 'storage'   , 'skykinetic.cpp')
]

# cykinetic_srclist = [
#      os.path.join(pkg_srcdirpath, 'cykinetic.pyx')
#     ,os.path.join(cpp_srcdirpath, 'storage'   , 'skykinetic.cpp')
# ]

skytether_inclist = [
     numpy.get_include()
    ,pyarrow.get_include()
    ,os.path.join(kinetic_installdir  , 'include')
    ,os.path.join(skytether_installdir, 'include')
]

skytether_liblist = [
     'arrow'
    ,'arrow_dataset'
    ,'arrow_python'
    ,'skytether'
]

skytether_libdirs = (
      [
           '/usr/lib'
          ,os.path.join(skytether_installdir, 'lib')
      ]
    + pyarrow.get_library_dirs()
)

# >> cython extension by source list
scytether_extension = Extension('scytether'
    ,scytether_srclist
    ,include_dirs=skytether_inclist
    ,library_dirs=skytether_libdirs
    ,libraries=skytether_liblist
)

# cykinetic_extension = Extension('cykinetic'
#     ,cykinetic_srclist
#     ,include_dirs=skytether_inclist
#     ,library_dirs=skytether_libdirs
#     ,libraries=skytether_liblist
# )


# ------------------------------
# Main build logic

with open('README.md', mode='r', encoding='utf-8') as readme_handle:
    readme_content   = readme_handle.read()


cythonize_modulelist = [ scytether_extension ]
# cythonize_modulelist = [ scytether_extension, cykinetic_extension ]
cythonize_compdirs   = { 'language_level': 3 }


setup(
     name             = 'skytether'
    ,version          = '0.1.0'
    ,zip_safe         = False
    ,author           = 'Aldrin Montana'
    ,author_email     = 'akmontan@ucsc.edu'
    ,description      = ('SkyhookDM for single-cell use cases')
    ,license          = 'MPL 2.0'
    ,keywords         = 'example documentation tutorial'
    ,url              = 'https://gitlab.com/skyhookdm/skytether-singlecell/'
    ,long_description = readme_content

    ,package_dir      = { '': py_srcdirpath }
    ,packages         = [ 'skytether', 'skytether.knowledge' ]
    # ,packages         = [ 'skytether', 'skytether.knowledge', 'skytether.cybindings' ]
    # ,package_data     = {
    #                         'skytether': [
    #                              'cybindings/scytether.pyx'
    #                             ,'cybindings/cykinetic.pyx'
    #                         ]
    #                     }
    ,ext_modules      = cythonize(
          module_list=cythonize_modulelist
         ,compiler_directives=cythonize_compdirs
     )

    ,classifiers      = [
          'Development Status :: 1 - Planning'
         ,'Topic :: Utilities'
         ,'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)'
     ]
)
