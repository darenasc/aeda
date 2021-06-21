from setuptools import find_packages, setup

setup(
    name = 'aeda',
    description = 'Automated Exploratory Data Analysis',
    find_packages = find_packages(where='src'),
    package_dir = {":src"},
    version = '0.0.1',
    author = 'Diego Arenas',
    author_email = 'darenasc@gmail.com'
)