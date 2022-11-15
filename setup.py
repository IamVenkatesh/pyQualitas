from setuptools import setup


setup(
    name="pyQualitas",
    version="1.0.5",
    description="A project to ensure the data quality using python",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Venkatesh Venkataramani",
    author_email="venkatesh.venkataramani@gmail.com",
    url="https://github.com/IamVenkatesh/pyQuality/wiki",
    packages=['pyqualitas', 'pyqualitas/checks', 'pyqualitas/checksuite', 'pyqualitas/utils'],
    package_dir={"":"src"},
    package_data={'pyqualitas/utils': ['template/TestResult.html']},
    install_requires=[
        "pyspark >= 3.3.0",
        "Jinja2 >= 3.1.2",
        "pandas >= 1.5.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Development Status :: 5 - Production/Stable",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Software Development :: Testing"
    ],
    include_package_data=True,
)