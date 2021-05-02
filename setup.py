from setuptools import setup

with open("requirements.txt") as f:
    requirements = f.readlines()

with open("README.md") as f:
    readme = f.read()

with open("HISTORY.md") as f:
    history = f.read()

setup(
    long_description_content_type="text/markdown",
    long_description=readme + "\n\n" + history,
    install_requires=requirements,
)
