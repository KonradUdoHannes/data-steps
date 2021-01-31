from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.readlines()

setup(
    long_description_content_type="text/markdown",
    install_requires=requirements
    )
