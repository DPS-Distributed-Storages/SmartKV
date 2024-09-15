from setuptools import setup, find_packages

setup(
    name='dml',
    version='0.0.1',
    packages=find_packages(),
    install_requires=['influxdb-client[async]', 'pymongo'],
    description=''
)
