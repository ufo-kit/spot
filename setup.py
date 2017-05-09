import os
from spot import __version__
from spot.loader import DATA_DIR
from setuptools import setup, find_packages


setup(
    name='spot',
    version=__version__,
    author='Matthias Vogelgesang',
    author_email='matthias.vogelgesang@kit.edu',
    url='http://github.com/ufo-kit/spot',
    license='LGPL',
    packages=find_packages(),
    data_files=[(DATA_DIR, [
        'data/ufo.json',
        'data/example.json'
        ])
    ],
    scripts=['bin/spot'],
    exclude_package_data={'': ['README.rst']},
    install_requires=[
        'jinja2'
    ],
    description="Spot",
)
