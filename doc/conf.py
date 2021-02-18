"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""
from documenteer.conf.pipelinespkg import *


project = "dbb_buffmngrs_endpoint"
html_theme_options["logotext"] = project
html_title = project
html_short_title = project

