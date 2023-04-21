from setuptools import setup, find_packages  # type: ignore
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    url="https://github.com/StepanGavrilov/PRQ",
    packages=find_packages(),
    author_email="gavrilovstepan78@gmail.com",
    name="prq",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="0.0.21",
    py_modules=["prq"],
    entry_points={
        "console_scripts": [
            "prq=prq:worker",
        ]
    },
    keywords=["python", "queue", "redis", "parallel", "workers", "tasks", "jobs"],
    install_requires=["rq >= 0.4.6", "gevent >= 1.0", "pydantic", "rich"],
)
