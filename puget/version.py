"""Version definition for puget."""
from os.path import join as pjoin

# Format expected by setup.py and doc/source/conf.py: string of form "X.Y.Z"
_version_major = 0
_version_minor = 1
_version_micro = ''  # use '' for first of series, number for 1 and above
_version_extra = 'dev'
# _version_extra = ''  # Uncomment this for full releases

# Construct full version string from these.
_ver = [_version_major, _version_minor]
if _version_micro:
    _ver.append(_version_micro)
if _version_extra:
    _ver.append(_version_extra)

__version__ = '.'.join(map(str, _ver))

CLASSIFIERS = ["Development Status :: 3 - Alpha",
               "Environment :: Console",
               "Intended Audience :: Science/Research",
               "License :: OSI Approved :: MIT License",
               "Operating System :: OS Independent",
               "Programming Language :: Python",
               "Topic :: Scientific/Engineering"]

# Description should be a one-liner:
description = "puget: HMIS data munging for puget sound"
# Long description will go up on the pypi page
long_description = """
Puget
========
Puget
To get started using these components in your own software, please go to the
repository README_.
.. _README: https://github.com/uwescience/shablona/blob/master/README.md
License
=======
``puget`` is licensed under the terms of the BSD license. See the file
"LICENSE" for information on the history of this software, terms & conditions
for usage, and a DISCLAIMER OF ALL WARRANTIES.
All trademarks referenced herein are property of their respective holders.
Copyright (c) 2015--, Ariel Rokem, The University of Washington eScience
Institute.
"""

NAME = "puget"
MAINTAINER = "Ariel Rokem"
MAINTAINER_EMAIL = "arokem@gmail.com"
DESCRIPTION = description
LONG_DESCRIPTION = long_description
URL = "http://github.com/uwescience-bmgf-hmis/puget"
DOWNLOAD_URL = ""
LICENSE = "BSD"
AUTHOR = "Ariel Rokem"
AUTHOR_EMAIL = "arokem@gmail.com"
PLATFORMS = "OS Independent"
MAJOR = _version_major
MINOR = _version_minor
MICRO = _version_micro
VERSION = __version__
PACKAGE_DATA = {'puget': [pjoin('data', '*'), pjoin('data', 'metadata', '*')]}
REQUIRES = ["numpy", "pandas"]
