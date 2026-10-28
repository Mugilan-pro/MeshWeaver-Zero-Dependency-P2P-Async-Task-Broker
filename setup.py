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
    description="A lightweight, decentralized peer-to-peer asynchronous task broker in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/meshweaver/meshweaver",
    packages=find_packages(include=["task*", "scheduler*", "gossip*", "dashboard*", "security*", "network*", "mesh*"]),
    py_modules=["config", "cli"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: System :: Distributed Computing",
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
    extras_require={
        "rich": ["rich>=12.0.0"],
        "cloudpickle": ["cloudpickle>=2.0.0"],
        "psutil": ["psutil>=5.8.0"],
    },
    entry_points={
        "console_scripts": [
            "meshweaver=cli:main",
        ],
    },
)
