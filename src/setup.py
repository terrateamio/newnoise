from setuptools import setup, find_packages

setup(
    name='newnoise',
    version='0.1.0',
    author='Jms Dnns',
    author_email='jdennis@gmail.com',
    description='It loads noisy data from one place, mutates it, and writes new noise to some other place.',
    packages=find_packages(),
    install_requires=[
    ],
    extras_require={
        'dev': [
        ], 
    },
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
    ],
    entry_points={
        'console_scripts': [
            'newnoise=newnoise.cli:run',
        ],
    },
)

