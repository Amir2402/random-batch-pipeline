from airflow.sdk import BaseOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage
from openlineage.client.generated.base import Dataset

class BaseOperatorWithLineage(BaseOperator):
    def __init__(self, lineage_output = None, lineage_input = None, **kwargs):
        super().__init__(**kwargs)
        self.lineage_input = lineage_input
        self.lineage_output = lineage_output
    
    def get_openlineage_facets_on_complete(self, task_instance):
        if self.lineage_input:
            return OperatorLineage(
                inputs=[
                    Dataset(
                        namespace=f"s3://{self.lineage_input["bucket"]}",
                        name=self.lineage_input["name"]
                    )
                ],
                outputs=[
                    Dataset(
                        namespace=f"s3://{self.lineage_output["bucket"]}",
                        name=self.lineage_output["name"]
                    )
                ]
            )
    
        else:
            return OperatorLineage(
                outputs=[
                    Dataset(
                        namespace=f"s3://{self.lineage_output["bucket"]}",
                        name=self.lineage_output["name"]
                    )
                ]
            )