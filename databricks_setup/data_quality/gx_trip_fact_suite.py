import great_expectations as gx
from great_expectations.core.batch import BatchRequest

context = gx.get_context()

batch_request = BatchRequest(
    datasource_name="trip_fact_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="workspace.default.trip_fact",
)

suite = context.create_expectation_suite("trip_fact_suite", overwrite_existing=True)
suite.add_expectation(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={"column": "trip_id"}
)

results = context.run_validation_operator(
    "action_list_operator", batch_request=batch_request, expectation_suite_name="trip_fact_suite"
)
print(results)
