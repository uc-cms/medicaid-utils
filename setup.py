import setuptools
import pathlib

HERE = pathlib.Path(__file__).parent
INSTALL_REQUIRES = (HERE / "requirements.txt").read_text().splitlines()

__version__ = "0.0.0"

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="medicaid_utils",
    version=__version__,
    author=(
        "Research Computing Group, Biostatistics Laboratory, The University of"
        " Chicago"
    ),
    author_email="manorathan@uchicago.edu",  # This should be changed to group email,
    description=(
        "Python toolkit for Medicaid claims data analysis — preprocessing, cleaning,"
        " risk adjustment, quality measures, and patient-level file construction"
        " for MAX and TAF CMS data"
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/uc-cms/medicaid-utils",
    packages=setuptools.find_packages(),
    include_package_data=True,
    package_data={"": ["data/*.csv"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=INSTALL_REQUIRES,
)
