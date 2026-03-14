"""Sphinx configuration for medicaid-utils documentation."""

import os
import pathlib
import re
import sys

sys.path.insert(0, os.path.abspath("../../../"))

# -- Project information -----------------------------------------------------

project = "medicaid-utils"
copyright = "2023-2026, Research Computing Group, Biostatistics Laboratory, The University of Chicago"
author = "Research Computing Group, Biostatistics Laboratory, The University of Chicago"

# Read version from setup.py (managed by python-semantic-release)
_setup_py = (pathlib.Path(__file__).resolve().parents[3] / "setup.py").read_text()
_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', _setup_py)
version = _match.group(1) if _match else "dev"
release = version

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx.ext.ifconfig",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
    "sphinx_sitemap",
]

templates_path = ["_templates"]
source_suffix = ".rst"
master_doc = "index"
language = "en"
exclude_patterns = []

# Google Search Console verification + sitemap
html_extra_path = ["googlef63886eaa754b15d.html"]
html_baseurl = "https://uc-cms.github.io/medicaid-utils/"
sitemap_url_scheme = "{link}"

pygments_style = "friendly"
pygments_dark_style = "monokai"

# -- Options for HTML output -------------------------------------------------

html_theme = "furo"
html_title = "medicaid-utils"
html_static_path = ["_static"]
html_favicon = "_static/favicon.svg"
html_css_files = ["custom.css"]

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#800000",
        "color-brand-content": "#800000",
    },
    "dark_css_variables": {
        "color-brand-primary": "#c26e6e",
        "color-brand-content": "#c26e6e",
    },
    "source_repository": "https://github.com/uc-cms/medicaid-utils",
    "source_branch": "master",
    "source_directory": "docs/sphinx/source/",
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/uc-cms/medicaid-utils",
            "html": (
                '<svg stroke="currentColor" fill="currentColor" stroke-width="0" '
                'viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0 '
                "3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 "
                "0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94"
                "-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 "
                "1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-"
                "3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 "
                ".67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 "
                "1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 "
                "1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 "
                "0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 "
                '8c0-4.42-3.58-8-8-8z"></path></svg>'
            ),
            "class": "",
        },
    ],
    "announcement": (
        "Now compatible with Python 3.13, Dask 2026.x, and Pandas 3.0 &mdash; "
        '<a href="https://pypi.org/project/medicaid-utils/">Install from PyPI</a>'
    ),
}

# -- Extension configuration -------------------------------------------------

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}
todo_include_todos = True
napoleon_custom_sections = ["References"]
