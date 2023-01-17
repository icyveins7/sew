#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 15 22:52:11 2023

@author: seoxubuntu
"""

from setuptools import setup, find_packages
setup(name="sew",
      version="0.1",
      packages=find_packages("."))

# from distutils.core import setup
# import os
# import re

# included_modules = os.listdir(os.path.dirname(os.path.abspath(__file__)))
# included_modules = [i for i in included_modules if re.match(r".+\.py", i)]
# included_modules.remove('__init__.py')
# included_modules.remove('setup.py')
# included_modules = [os.path.splitext(i)[0] for i in included_modules]
# print(included_modules)

# setup(name="sew",
#       version="0.1",
#       py_modules=included_modules
# )
