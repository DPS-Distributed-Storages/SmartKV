from setuptools import setup, find_packages

setup(
    name='optimizer',
    version='0.0.1',
    packages=find_packages(),
    install_requires=['influxdb-client[async]', 'pymongo', 'dml', 'pandas', 'networkx', 'scikit-learn', 'matplotlib'],
    description=''
)
