from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session
import boto3
from mlflow import MlflowClient
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
import os

def invoke_step_function():
    client = boto3.client('stepfunctions')
    
    response = client.start_execution(
        stateMachineArn='arn:aws:states:us-east-1:843412435203:stateMachine:MyStateMachine-48o66rm2k',
        name='Executado_pelo_data_drift',  # Um nome único para a execução
        input='{"input_key": "input_value"}'  # Dados de entrada para a Step Function
    )

    # Se necessário, você pode lidar com a resposta aqui
    print(response)

model_name = 'teste-svm'
client = MlflowClient()
production_model = next(filter(lambda x:x.current_stage=='Production', client.search_model_versions("name='teste-svm'")))

prefix = 'sagemaker-featurestore-introduction'
role = os.environ.get('aws_role')

sagemaker_session = Session()
region = sagemaker_session.boto_region_name
s3_bucket_name = sagemaker_session.default_bucket()

feature_group_name = 'test-feature-group-30-17-09-25'


feature_group = FeatureGroup(
    name=feature_group_name, sagemaker_session=sagemaker_session
)

query = feature_group.athena_query()
table_name = query.table_name

query_string_reference = (f"""
    SELECT  *
    FROM    "sagemaker_featurestore"."{table_name}"
    WHERE   CAST(from_unixtime(DATE) AS DATE) <= DATE(from_unixtime({production_model.creation_timestamp}/1000));
""")

query.run(query_string=query_string_reference,
          output_location=f's3://{s3_bucket_name}/{prefix}/query_results/')
query.wait()
dataset_reference = query.as_dataframe()
dataset_reference = dataset_reference[['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'target_name']]

query_string_current = (f"""
    SELECT  *
    FROM    "sagemaker_featurestore"."{table_name}"
    WHERE   CAST(from_unixtime(DATE) AS DATE) >= DATE(from_unixtime({production_model.creation_timestamp}/1000));
""")

query.run(query_string=query_string_current,
          output_location=f's3://{s3_bucket_name}/{prefix}/query_results/')
query.wait()
dataset_current = query.as_dataframe()
dataset_current = dataset_current[['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'target_name']]

data_drift_report = Report(metrics=[
    DataDriftPreset(),
])

data_drift_report.run(reference_data=dataset_reference, current_data=dataset_current)
if data_drift_report.as_dict()['metrics'][0]['result']['number_of_drifted_columns'] < 1:
    print('invoke step function')
    invoke_step_function()
else:
    print('not invoke step function')
