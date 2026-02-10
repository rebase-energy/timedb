# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import os
import sys
sys.path.insert(0, os.path.abspath('..'))

# Copy notebook files from examples directory to docs/notebooks for nbsphinx
import shutil
import glob

_docs_dir = os.path.dirname(os.path.abspath(__file__))
_examples_src = os.path.join(_docs_dir, '..', 'examples')
_notebooks_dir = os.path.join(_docs_dir, 'notebooks')

# Create notebooks directory if it doesn't exist
os.makedirs(_notebooks_dir, exist_ok=True)

# Copy all .ipynb files from examples to docs/notebooks
for nb_file in glob.glob(os.path.join(_examples_src, '*.ipynb')):
    basename = os.path.basename(nb_file)
    dest = os.path.join(_notebooks_dir, basename)
    # Only copy if source is newer or dest doesn't exist
    if not os.path.exists(dest) or os.path.getmtime(nb_file) > os.path.getmtime(dest):
        shutil.copy2(nb_file, dest)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'timedb'
copyright = '2024, timedb contributors'
author = 'timedb contributors'
release = '0.1.1'
version = '0.1.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autosummary',
    'nbsphinx',
]

# nbsphinx configuration
nbsphinx_execute = 'never'  # Don't execute notebooks during build
nbsphinx_allow_errors = True  # Continue build even if notebooks have errors

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# ReadTheDocs theme options
html_theme_options = {
    'navigation_depth': 2,
}

# -- Extension configuration -------------------------------------------------

# Napoleon settings for NumPy/Google style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True

# Intersphinx mapping
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pandas': ('https://pandas.pydata.org/docs/', None),
    'pint': ('https://pint.readthedocs.io/en/stable/', None),
}

