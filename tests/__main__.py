#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 19:33:25 2023

@author: seoxubuntu

Run this from the outer sew folder with 'python -m tests'
"""

import unittest

print("Begin tests")

# Import statements unittests
print("Running statements unittests")
from .statements import *

# Import columns unittests
print("Running columns unittests")
from .columns import *

# Import correctness unittests
print("Running correctness unittests")
from .correctness import *

# Import plugins unittests
print("Running plugins unittests")
from .plugins import *

# Import blob unittests
print("Running blob unittests")
from .blobs import *

# Run everything
unittest.main()
