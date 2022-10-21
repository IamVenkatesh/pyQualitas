from setuptools import setup


with open("README.md", "r") as readfile:
    long_description = readfile.read()
readfile.close()

setup(
    name='main',
    version='0.0.1',
    description='A Data Quality framework using pyspark',
    author='Venkatesh Venkataramani',
    maintainer='Venkatesh Venkataramani',
    author_email='venkatesh.venkataramani@gmail.com',
    url='https://github.com/IamVenkatesh/pyvalidator/wiki',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=["pyspark >= 3.3.0", "plotly >= 5.10.0", "pandas >= 1.5.0"],
    packages=['main'],
    package_dir={'main': 'src/main'},
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Quality Assurance :: Data Quality'
    ]
)
