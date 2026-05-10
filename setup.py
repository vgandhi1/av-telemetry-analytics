from setuptools import setup, find_packages

setup(
    name="av-telemetry-analytics",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "confluent-kafka>=2.3.0",
        "pyspark>=3.5.0",
        "duckdb>=0.10.0",
        "pyarrow>=15.0.0",
        "boto3>=1.34.0",
        "pandas>=2.2.0",
        "numpy>=1.26.0",
        "scikit-learn>=1.4.0",
        "pyyaml>=6.0.1",
        "pydantic>=2.6.0",
        "structlog>=24.1.0",
    ],
    entry_points={
        "console_scripts": [
            "av-ingest=ingestion_pipeline:main",
            "av-process=stream_processing:main",
            "av-dashboard=src.monitoring.dashboard:main",
        ],
    },
)
