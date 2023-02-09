#!/bin/bash


# ------------------------------
# Overview

# This script is meant to be run from the repository root, where there should be two files that
# define builds: `meson.build` for C++ and `setup.py` for python/cython.

# As of this writing, this script is located in the `scripts/` directory, but should be ran from
# one directory higher, where there is also a `src` directory. This script is located in the
# `scripts/` directory to keep the root directory relatively clean.


# ------------------------------
# Global variables

# this matches the python build path for convenience
build_dirpath="build"


# ------------------------------
# C++ build logic

# >> Configure the build
# setup the install prefix and make sure ubuntu just uses a 'lib' subdir
if [[ ! -d "${build_dirpath}" ]]; then
    meson --prefix=/opt/skytether-singlecell \
          --libdir=lib                       \
          "${build_dirpath}"
fi


# >> Compile
# `-C <dirpath>` runs `ninja` from the given <dirpath>
# so this is basically `cd build-dir && ninja`
meson compile -C "${build_dirpath}"


# >> Install files to install prefix
# similar to compile step above: `cd build-dir && ninja install`
# if this isn't run as root, it should be run with sudo privileges
# (or the install prefix should be given appropriate permissions)
meson install -C "${build_dirpath}"


# ------------------------------
# Python build logic

python setup.py clean build_ext install
