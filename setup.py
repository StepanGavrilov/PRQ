from setuptools import setup, find_packages  # type: ignore

setup(
    packages=find_packages(),
    author_email="gavrilovstepan78@gmail.com",
    name="prq",
    version="0.0.12",
    py_modules=["prq"],
    entry_points={
        "console_scripts": [
            "prq=prq:worker",
        ]
    },
    keywords=["python", "queue", "redis", "parallel", "workers", "tasks", "jobs"],
    install_requires=["rq >= 0.4.6", "gevent >= 1.0"],
)
