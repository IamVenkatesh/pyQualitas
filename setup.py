from setuptools import setup


with open("README.md", "r") as readfile:
    long_description = readfile.read()
readfile.close()

setup(
    name='pyQuality',
    version='0.0.1',
    description='A Data Quality framework using pyspark',
    author='Venkatesh Venkataramani',
    author_email='venkatesh.venkataramani@gmail.com',
    url='https://github.com/IamVenkatesh/pyvalidator/wiki',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=["pyspark >= 3.3.0", "plotly >= 5.10.0"]
)