#!/usr/bin/env python
import sys

from pyartcdlib.pipelines.prepare_release import main

if __name__ == "__main__":
    exit(main(sys.argv[1:]))
