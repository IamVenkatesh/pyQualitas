from setuptools import setup, find_packages


setup(
    name="pyQualitas",
    version="1.0.0",
    description="A project to ensure the data quality using python",
    long_description=open("README.md").read(),
    author="Venkatesh Venkataramani",
    author_email="venkatesh.venkataramani@gmail.com",
    url="https://github.com/IamVenkatesh/pyQuality/wiki",
    packages=find_packages(include=["pyqualitas", "pyqualitas/*"]),
    package_dir={'pyqualitas': 'src/pyqualitas'},
    package_data={'pyqualitas': ['pyqualitas/utils/template/TestResult.html']},
    install_requires=[
        "pyspark >= 3.3.0",
        "Jinja2 >= 3.1.2",
        "pandas >= 1.5.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Software Development :: Testing"
    ],
    include_package_data=True,
)