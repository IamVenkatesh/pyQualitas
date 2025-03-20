from setuptools import setup


setup(
    name="pyQualitas",
    version="2.0.0",
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
        "pyspark = 3.5.5",
        "Jinja2 = 3.1.6",
        "pandas = 2.2.3",
        "slack-sdk = 3.35.0",
        "pymsteams = 0.2.5",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Development Status :: 5 - Production/Stable",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Software Development :: Testing"
    ],
    include_package_data=True,
)