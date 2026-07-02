from setuptools import find_packages, setup


setup(
    name="llm-api-router",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.110",
        "uvicorn>=0.27",
        "httpx>=0.26",
        "PyYAML>=6.0",
    ],
    extras_require={"test": ["pytest>=7", "pytest-asyncio>=0.21"]},
    entry_points={"console_scripts": ["llm-router=llm_api_router.cli:main"]},
)
