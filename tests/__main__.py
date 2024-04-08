#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 19:33:25 2023

@author: seoxubuntu

Run this from the outer sew folder with 'python -m tests'
"""

from .blobs import *
from .plugins import *
from .formatSpecifier import *
from .correctness import *
from .columns_conditions import *
from .statements import *
import unittest

print("Begin tests")

# Import statements unittests
print("Running statements unittests")

# Import columns & conditions unittests
print("Running columns & conditions unittests")

# Import correctness unittests
print("Running correctness unittests")

# Import formatSpecifier unittests
print("Running formatSpecifier unittests")

# Import plugins unittests
print("Running plugins unittests")

# Import blob unittests
print("Running blob unittests")

# Run everything
unittest.main()
