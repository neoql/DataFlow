from setuptools import setup

setup(
    name='dataflow',
    version='0.1.0',
    description='A lightweight data stream processing framework.',
    author='Neo',
    tests_require=['pytest'],
    extras_require={'pbar': ['tqdm']},
    packages=['dataflow']
)
