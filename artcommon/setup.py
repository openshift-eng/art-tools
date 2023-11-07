# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
import sys
if sys.version_info < (3, 8):
    sys.exit('Sorry, Python < 3.8 is not supported.')

setup(
    name="artcommon",
    author="AOS ART Team",
    author_email="aos-team-art@redhat.com",
    description="Common library files used by ART tools",
    url="https://github.com/openshift-eng/art-tools/tree/main/artcommon",
    license="Apache License, Version 2.0",
    packages=find_packages(exclude=["tests", "tests.*"]),
    dependency_links=[],
    python_requires='>=3.8',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Environment :: Console",
        "Operating System :: POSIX",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "Natural Language :: English",
    ]
)
