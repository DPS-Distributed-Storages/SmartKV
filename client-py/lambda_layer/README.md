# Create a Lambda layer

Run the build script in a docker container:
```
docker run --volume ${PWD}/..:/client-py --workdir /build --rm "public.ecr.aws/sam/build-python3.9" /client-py/lambda_layer/build.sh "py3.9"
```

Upload the .zip file containing the layer package to AWS Lambda.
