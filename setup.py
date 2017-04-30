from distutils.core import setup

from setuptools import find_packages

setup(
    name='katagawa',
    version='0.1.0',
    packages=find_packages(),
    url='https://github.com/SunDwarf/Katagawa',
    license='MIT',
    author='Laura Dickinson',
    author_email='l@veriny.tf',
    description='An asyncio ORM for Python 3.5',
    install_requires=[
        "cached_property==1.3.0"
    ]
)
