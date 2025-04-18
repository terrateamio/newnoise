from setuptools import find_packages, setup

setup(
    name="newnoise",
    version="0.1.1",
    author="Jms Dnns",
    author_email="jdennis@gmail.com",
    description="It loads noisy data from one place, mutates it, and writes new noise to some other place.",
    packages=find_packages(),
    install_requires=[
        "json_stream",
        "aiohttp",
        "aiodns",
    ],
    extras_require={
        "dev": [],
    },
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
    ],
    entry_points={
        "console_scripts": [
            "newnoise=newnoise.cli:run",
        ],
    },
)
