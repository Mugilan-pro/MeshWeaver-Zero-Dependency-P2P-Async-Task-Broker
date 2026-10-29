"""
MeshWeaver: Zero-Dependency P2P Async Task Broker
Setup configuration for packaging and edge deployment.
"""

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="meshweaver",
    version="1.0.0",
    author="MeshWeaver Distributed Systems Engineering Team",
    author_email="sujithkumarsingh422@gmail.com",
    description="A pure-Python, zero-dependency decentralized P2P async task broker for edge computing.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Mugilan-pro/MeshWeaver-Zero-Dependency-P2P-Async-Task-Broker",
    packages=find_packages(exclude=["tests*", "examples*"]),
    py_modules=["config", "meshweaver_cli", "cli"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Networking",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        # Zero required external dependencies — runs entirely on Python standard library!
    ],
    extras_require={
        "rich": ["rich>=12.0.0"],
        "cloudpickle": ["cloudpickle>=2.0.0"],
        "psutil": ["psutil>=5.8.0"],
        "enhanced": ["cloudpickle>=2.0.0", "psutil>=5.8.0", "rich>=12.0.0"],
    },
    entry_points={
        "console_scripts": [
            "meshweaver=meshweaver_cli:main",
            "meshweaver-node=cli:main",
        ],
    },
)
