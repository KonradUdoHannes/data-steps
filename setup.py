from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.readlines()

setup(
    install_requires=requirements
    )
