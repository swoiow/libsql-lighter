from pathlib import Path

from setuptools import find_packages, setup


ROOT = Path(__file__).parent
README = ROOT / "README.md"
REQS = ROOT / "requirements.txt"

long_description = README.read_text(encoding="utf-8") if README.exists() else ""
install_requires = REQS.read_text(encoding="utf-8").splitlines() if REQS.exists() else []

setup(
    name="llibsql-adapter",
    version="0.0.1",
    description="Lightweight adapter: pandas DataFrame <-> libsql (commit + sync).",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="HarmonSir",
    author_email="git@pylab.me",
    url="https://github.com/swoiow/llibsql-adapter",
    license="MIT",

    packages=find_packages(include=["llibsql_adapter", "llibsql_adapter.*"]),
    include_package_data=True,
    zip_safe=False,

    python_requires=">=3.12",
    install_requires=install_requires,

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords=["libsql", "turso", "sqlite", "pandas", "dataframe", "adapter"],
    project_urls={
        "Source": "https://github.com/swoiow/llibsql-adapter",
        "Issues": "https://github.com/swoiow/llibsql-adapter/issues",
    },
)
