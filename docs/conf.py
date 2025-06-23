# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "gridstatus"
copyright = "2023, Max Kanter"

master_doc = "index"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_design",
    "autoapi.extension",
    # "sphinx_tabs.tabs",
    "sphinx_thebe",
    "sphinx_togglebutton",
    "sphinxext.opengraph",
    "sphinx_favicon",
    "sphinx.ext.inheritance_diagram",
    "sphinxext.opengraph",
    "sphinx.ext.napoleon",
]

templates_path = ["_templates"]
exclude_patterns = ["autoapi", "_autoapi_templates", "_build", "Thumbs.db", ".DS_Store"]

language = "en"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_logo = "_static/favicon.png"
html_title = "gridstatus"
html_static_path = ["_static"]
html_css_files = ["custom.css"]

html_theme_options = {
    "path_to_docs": "docs",
    "repository_url": "https://github.com/gridstatus/gridstatus",
    # "repository_branch": "gh-pages",  # For testing
    "launch_buttons": {
        "binderhub_url": "https://mybinder.org",
        "colab_url": "https://colab.research.google.com/",
        "deepnote_url": "https://deepnote.com/",
        "notebook_interface": "jupyterlab",
        "thebe": True,
        # "jupyterhub_url": "https://datahub.berkeley.edu",  # For testing
    },
    # "use_edit_page_button": True,
    "use_issues_button": True,
    "use_repository_button": True,
    "use_download_button": True,
    "use_sidenotes": True,
    "show_toc_level": 2,
    "show_navbar_depth": 2,
    # "announcement": (
    #     "⚠️The latest release refactored our HTML, "
    #     "so double-check your custom CSS rules!⚠️"
    # ),
    # For testing
    "use_fullscreen_button": False,
    # "home_page_in_toc": True,
    # "single_page": True,
    # "extra_footer": "<a href='https://google.com'>Test</a>",  # DEPRECATED KEY
    # "extra_navbar": "<a href='https://google.com'>Test</a>",
}

# -- Options for todo extension ----------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/todo.html#configuration

todo_include_todos = True

# -- Options for sphinx_favicon ----------------------------------------------
favicons = [
    {
        "rel": "icon",
        "static-file": "icon.ico",  # => use `_static/icon.svg`
        "type": "image/ico",
    },
    {
        "sizes": "16x16",
        "static-file": "favicon-16x16.png",
    },
    {
        "sizes": "32x32",
        "static-file": "favicon-32x32.png",
    },
    {
        "rel": "apple-touch-icon",
        "sizes": "180x180",
        "static-file": "apple-touch-icon.png",
    },
]


# -- Options for notebook output ---------------------------------------------
nb_execution_excludepatterns = ["Examples/*", "Examples/*/*"]
source_suffix = {
    ".rst": "restructuredtext",
    ".ipynb": "myst-nb",
    ".myst": "myst-nb",
}
nb_execution_mode = "cache"

autoapi_type = "python"
autoapi_dirs = ["../gridstatus"]
autoapi_add_toctree_entry = False
autoapi_template_dir = "_autoapi_templates"
autoapi_python_class_content = "both"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
    "imported-members",
]
autodoc_inherit_docstrings = True
suppress_warnings = [
    "mystnb.unknown_mime_type",
    "autoapi",
]
html_js_files = [
    "https://cdnjs.cloudflare.com/ajax/libs/require.js/2.3.4/require.min.js",
]


# -- Options for open graph ------------------------------------------------

ogp_site_url = "https://www.gridstatus.io"
# TODO: this image does not exist
ogp_image = "https://opensource.gridstatus.io/en/latest/_static/grid-status-og.jpg"
