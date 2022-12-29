from __future__ import annotations

from pathlib import Path

import setuptools


def read_multiline_as_list(file_path: Path | str) -> list[str]:
    with open(file_path) as req_file:
        contents = req_file.read().split("\n")
        if contents[-1] == "":
            contents.pop()
        return contents


requirements = read_multiline_as_list("requirements.txt")

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name="migo",
    version="0.0.1",
    author="Teialabs",
    author_email="severo@teialabs.com",
    description="Milvus and Mongo orchestrator to use the best of both worlds",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TeiaLabs/migo",
    packages=setuptools.find_packages(),
    keywords="milvus vector document mongo database",
    python_requires=">=3.8",
    install_requires=requirements,
)