FROM public.ecr.aws/lambda/python:3.11

# app files
COPY clinical_trials_api.py requirements.txt ./

# install deps into Lambda task root
RUN pip install --no-cache-dir -r requirements.txt -t "${LAMBDA_TASK_ROOT}"

# Lambda entrypoint: module.function
CMD ["clinical_trials_api.lambda_handler"]
