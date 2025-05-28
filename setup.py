from setuptools import setup, find_packages

setup(
    name='analytics_utilities',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'statsmodels'
    ],
    author='saramcneilly',
    description='A collection of some of my most common utilities for doing an analysis on an A/b test',
    url='https://github.com/sarasite/analytics_utilities',
)