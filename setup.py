from setuptools import setup, find_packages

setup(
    name = 'aeda',
    description = 'Automated Exploratory Data Analysis',
    packages = find_packages(where='src'),
    package_dir = {"":"src"},
    version = '0.0.1',
    author = 'Diego Arenas',
    author_email = 'darenasc@gmail.com'
)