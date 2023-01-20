#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 19:33:25 2023

@author: seoxubuntu

Run this from the outer sew folder with 'python -m tests'
"""

import unittest

# import ..sew
print("Running tests")

# Import benchmark unittests
from .benchmarks import *

# Run everything
unittest.main()
