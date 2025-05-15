from setuptools import setup, find_packages

setup(
    name="anonymization-pipeline",
    version="0.1.0",
    description="Data pipeline to anonymize and process persons data from Faker API.",
    author="Samuel Tseng",
    author_email="samuel.tseng@icloud.com",
    packages=find_packages(),
    python_requires=">=3.12",
    install_requires=[
        "requests==2.32.3",
        "duckdb==1.2.2",
        "pandas==2.2.3",
    ],
    extras_require={
        "dev": [
            "pytest==8.3.5",
            "pytest-cov==6.1.1",
            "flake8==7.2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "anonymization-pipeline=pipeline.main:main",
        ],
    },
)
