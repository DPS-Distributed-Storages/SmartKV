#!/bin/bash

FILENAME="dml-lambda-layer-${1}.zip"

pip install /client-py/ --target python
zip -r /client-py/lambda_layer/"${FILENAME}" python

rm -rf build python
