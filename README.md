## Project Overview

In this project we will ingest the CSV files for an insurance company from an on-premises computer/server and store it into Microsoft Fabric for storage, processing and reporting.

Technology: SPARK, Fabric Data Factory, Great Expectations, DBT Core, SqlFluff.

Note: This project assume you have an understanding of Microsoft Fabric and DBT core

## Content: <br />

- [Fabric Setup](#fabric-setup)
- [Data Ingestion](#data-injestion)
- [Data Processing](#data-processing)
  - [Data Cleaning](#data-cleaning)
  - [DBT Data validation and Transformation](#dbt-data-validation-and-transformation)
- [Putting It Together](#putting-it-together)
- [Reporting and Analyses](#reporting-and-analyses)
- [Continuous Integration Continuous Delivery (CICD)](#continuous-integration-continuous-delivery)

## Fabric Setup

> Go to settings -> Manage connections and gateways and create a connection to an on-premises computer.

![ALT](connection_01.png)
![ALT](connection_02.png)
![ALT](connection_03.png)

> Create a developement workspace.

![ALT](workspace.png)

> Create a lakehouse in the workspace.

![alt text](Lakehouse.png)

> Create a schema in the lakehouse to organise tables.

![alt text](lakehouse_schema.png)

> Create subdirectories in the lakehouse to store the ingested files.

![alt text](lakehouse_subfolder.png)

> Create a warehouse in the workspace.

![alt text](warehouse.png)

> Create schemas in the warehouse for report and analyses.

![alt text](warehouse_schemas.png)

## Data Ingestion

> Create a pipeline in the workspace and add a Metadata activity to it.

![alt text](metadata.png)

→ Use the connection created earlier in the activity and chose 'child items' as a 'field list' argument

> Add a ForEach activity to the pipeline and connect the Metadata activity to it.

![alt text](ForEach_01.png)

→ In setting -> tick the sequential option and add the dynamic expression in the items fields as shown in the image.

> Add a Copy activity into the ForEach activity and configure it as shown in the images.

![alt text](ForEach_Copy_01.png)
![alt text](ForEach_Copy_01.png)

> Add a Enviromnent item to the the workspace and add the Python Great Expectations to the Environment item.

![alt text](env_01.png)
![alt text](env_02.png)

→ Click the publish button to install the library

## Data Processing

> Create a notebook in the workspace.

→ Add the following code in a cell in the notebook to read parquet files from lakehouse subdirectory and load them into dataframes.

```
# ===============================
# 1. LOAD RAW DATA (Bronze Layer)
# ===============================
df_customers_raw = spark.read.parquet('abfss://ws_insurance_dev@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/insurance_raw/customers.parquet')
df_policies_raw  = spark.read.parquet('abfss://ws_insurance_dev@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/insurance_raw/policies.parquet')
df_claims_raw    = spark.read.parquet('abfss://ws_insurance_dev@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/insurance_raw/claims.parquet')

```

→ Add the following code in another cell in the notebook to build helper function with Great Expectations library for schema validation.

```
import great_expectations as gx
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col

def validate_schema_with_gx(
    df: DataFrame,
    schema: dict,
    expected_row_count: int = None,
    check_ordered_columns: bool = True,
    enable_length_check: bool = False
) -> bool:
    ''' Function to validates input dataframe against a schema using Great Expectations.
    Input: df (dataframe), schema (dict), expected_row_count (int), check_ordered_columns (bool), enable_length_check (bool)
    Output: bool
    '''

    """
    Runs Great Expectations checks on a Spark DataFrame.
    """


    # 1) Build a transient GX context and Spark datasource
    context = gx.get_context()
    ds = context.data_sources.add_spark(name="spark_in_memory")
    asset = ds.add_dataframe_asset(name="df_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("df_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    """
    # 1) Build a transient GX context and Pandas Dataframe datasource
    context = gx.get_context()
    datasource = context.data_sources.add_pandas(name="pandas_datasource")
    name = "df_dataframe"
    data_asset = datasource.add_dataframe_asset(name=name)
    batch_definition_name = "df_batch"
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
    )
    batch_parameters = {"dataframe": df}

    # Get the dataframe as a Batch
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    """
    # 2) Run expectations per schema
    from great_expectations import expectations as E
    results = []
    ordered_cols = []
    for col, props in schema.items():
        ordered_cols.append(col)

        if props.get("unique", False):
            results.append(batch.validate(E.ExpectColumnValuesToBeUnique(column=col)))
        if props.get("nullable", True) is False:
            results.append(batch.validate(E.ExpectColumnValuesToNotBeNull(column=col)))

        dtype = props.get("dtype")
        if dtype:
            results.append(batch.validate(E.ExpectColumnValuesToBeOfType(column=col, type_=dtype)))

        if enable_length_check:
            size = props.get("size")
            if size is not None:
                results.append(
                    batch.validate(
                        E.ExpectColumnValueLengthsToBeBetween(
                            column=col, min_value=None, max_value=int(size), strict_max=True
                        )
                    )
                )

    # 3) Table-level expectations
    if check_ordered_columns:
        results.append(batch.validate(E.ExpectTableColumnsToMatchOrderedList(column_list=ordered_cols)))
    if expected_row_count is not None:
        results.append(batch.validate(E.ExpectTableRowCountToEqual(value=int(expected_row_count))))

    # 4) Summarize results
    temp = list()
    total = len(results)
    for r in results:
        if getattr(r, "success", False):
            temp.append(1)

    successes = len(temp)
    #successes = sum(1 for r in results if getattr(r, "success", False))
    failures = total - successes

    #total = len(results)
    #successes = sum(1 for r in results if getattr(r, "success", False))
    #failures = total - successes

    print(f"[DQ] Expectations run: {total} | Passed: {successes} | Failed: {failures}")
    if failures > 0:
        for r in results:
            if not getattr(r, "success", False):
                cfg = getattr(r, "expectation_config", None)
                etype = getattr(cfg, "type", "unknown") if cfg else "unknown"
                kwargs = getattr(cfg, "kwargs", {}) if cfg else {}
                print(f"[DQ][FAIL] {etype} {kwargs}")
        print("[DQ] Data Quality validation failed.")
        return False
        #raise Exception("Data Quality validation failed.")
    else:
        print("[DQ] All checks passed ✔️")
        return True


```

→ Add the following code in another cell in the notebook and add the PySpark codeto create the schema definitions to validate the data against.

```
# Expect the columns to be from the expected column set
spark_customers_expected_schema = {
    "cust_id":         {"size": 2, "dtype": "StringType",  "unique": True,  "nullable": True},
    "fullname":       {"size": 255,  "dtype": "StringType",   "unique": False, "nullable": True},
    "date_of_birth":       {"size": 50,  "dtype": "StringType",   "unique": False, "nullable": True},
    "Gender":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "contact_number":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},

}

spark_claims_expected_schema = {
    "claim_id":         {"size": 3, "dtype": "StringType",  "unique": True,  "nullable": True},
    "policy_id":       {"size": 2,  "dtype": "StringType",   "unique": False, "nullable": True},
    "claim_date":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "claim_amount":       {"size": 10,  "dtype": "StringType",   "unique": False, "nullable": True},
    "status":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "Gender":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "deductible":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "claim_reason":       {"size": 50,  "dtype": "StringType",   "unique": False, "nullable": True},
    "is_fraud":       {"size": 5,  "dtype": "StringType",   "unique": False, "nullable": True},
    "Zip_Code":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
}

spark_policies_expected_schema = {
    "policy_id":         {"size": 2, "dtype": "StringType",  "unique": True,  "nullable": True},
    "cust_id":       {"size": 2,  "dtype": "StringType",   "unique": False, "nullable": True},
    "policy_type":       {"size": 50,  "dtype": "StringType",   "unique": False, "nullable": True},
    "status":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "start_date":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "end_date":       {"size": 25,  "dtype": "StringType",   "unique": False, "nullable": True},
    "coverage_amount":       {"size": 10,  "dtype": "StringType",   "unique": False, "nullable": True},
    "annual_premium":       {"size": 10,  "dtype": "StringType",   "unique": False, "nullable": True},
    "monthly_payment":       {"size": 10,  "dtype": "StringType",   "unique": False, "nullable": True},
    "distribution_channel":       {"size": 10,  "dtype": "StringType",   "unique": False, "nullable": True},
}


```

→ Add the following code in another cell in the notebook add the PySpark code to get the datasets records count.

```
customers_expected_rows = df_customers_raw.count()
policies_expected_rows = df_policies_raw.count()
claims_expected_rows = df_claims_raw.count()

```

→ Add the following code in another cell in the notebook add the PySpark code to validate the dataframes against the schema definitions.

```
# TO USE WITH A COMPUTE CONFIGURATION
customers_validated = validate_schema_with_gx(
    df=df_customers_raw,
    schema=spark_customers_expected_schema,
    expected_row_count=customers_expected_rows,
    check_ordered_columns=True,
    enable_length_check=False
)


policies_validated = validate_schema_with_gx(
    df=df_policies_raw,
    schema=spark_policies_expected_schema,
    expected_row_count=policies_expected_rows,
    check_ordered_columns=True,
    enable_length_check=False
)

claims_validated = validate_schema_with_gx(
    df=df_claims_raw,
    schema=spark_claims_expected_schema,
    expected_row_count=claims_expected_rows,
    check_ordered_columns=True,
    enable_length_check=False
)

```

→ Add the following code in another cell in the notebook add the PySpark code to save the validated as delta lake tables in the lakehouse.

```
if customers_validated:
    df_customers_raw.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("bronze.customers")
else:
    raise Exception("Bronze customers table schema validation failed")

if policies_validated:
    df_policies_raw.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("bronze.policies")
else:
    raise Exception("Bronze policies table schema validation failed")

if claims_validated:
    df_claims_raw.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("bronze.claims")
else:
    raise Exception("Bronze claims table schema validation failed")

```

### Data Cleaning

> We will also clean the data in this layer in notebook since Pysaprk is better suited to the cleaning and data transformation activities needed for data processing.

> We will perform the following cleaning activities (Code in notebook -process data).

> Customer data

- Drop rows without customer ID
- Convert customer ID datatype to integer
- Capitalize first letter of names in customer name field
- Remove duplicated rows

> Policies data

- Standardize date format for start and end date fields and convert to date data type
- Convert coverage amount, annual premium, and monthly payments fields datatype to double
- Capitalize first letter of values in status and policy type fields
- Drop record without customer ID and policy ID fields

> Claims data

- Standardize date format for claim date field and convert to date data type
- Convert claim amount, and deductible fields datatype to double
- Capitalize first letter of values in status and gender fields
- Drop record without claims ID fields
- Remove duplicated rows

### DBT Data validation and Transformation

We created a DBT project to validate and transform the data for the silver and gold layers in medallion architecture of the Microsoft Fabric project. It uses the Slowly Changing Dimensio (SCD) type data modelling technique to model the dimension tables in this layer.

We will use Microsoft Azure Service Principal to connect to our Warehouse in Microsoft Fabric.

We assume you have a Microsoft Azure Service Principal Account added as a Member/Contributor role in the Microsoft Fabric workspace.

In the DBT project we do the following:

- Update the statistics on gold layer tables after DBT builds
- Create SCD type 2 modelling for the customers and policies snapshots
- Create a Incremental claims and master data models
- Create views for the polices and customers data in the gold layer
- Validation:
  - Apply unique constrainst to key columns
  - Apply not null constrainst to key columns
  - Apply accepted values constrainst selected fields - status, gender etc
  - Apply relationship constraints between the tables
  - Test money value fields have values greater than 0
  - Test string values are not empty
  - Test date fields have no future date values
- Linting test

The DBT projec:

![ALT](dbt_project_01_ann.png)
![ALT](dbt_project_02_ann.png)

We want to create a basic CI pipeline in GitHub to run tests and build the project. This will ensure we are using tested DBT project in Microsoft Fabric.
We will perform the followin in the pipeline

1. SQL linting (we use [sqlfluff](https://www.sqlfluff.com/))
2. dbt testing
3. dbt build/run

> Sqlfluff installation

![ALT](sqlfluff.png)

> Sqlfuff configuration

![ALT](sqlfluff_config_01.png)
![ALT](sqlfluff_config_02.png)

The CI Pipeline

![ALT](cicd_01.png)
![ALT](cicd_02.png)
![ALT](cicd_03.png)
![ALT](cicd_04.png)

We set the repository secret variables to execute the GitHub runner.

![ALT](github_secrets.png)

On push to main branch. We can see the logs

![AL](cicd_success.png)
![ALT](cicd_test.png)

We can now create a DBT job in Microsoft Fabric and connect the the DBT project Github repository to run the DBT project.

![ALT](DBT_job.png)
![ALT](DBT_connect_github.png)

Select your branch in GitHub and your Warehouse in Microsoft Fabric.

Run the job to test it.

![ALT](DBT_test_run.png)

The full DBT code can be found [here](https://github.com/salano/github-insurance-ci-project)

## Putting It Together

In your warehouse create materialized tables for your customers and policies views since views cannot be selected on delta lake in Microsoft Fabric for Semantic models.

```

CREATE TABLE [gold].[customers_materialized_table]
AS
SELECT * FROM [gold].[vw_customers]
WHERE 1 = 0; -- Creates the structure empty


CREATE TABLE [gold].[policies_materialized_table]
AS
SELECT * FROM [gold].[vw_policies]
WHERE 1 = 0; -- Creates the structure empty

```

Create customers and polices stored procedures to refresh the materialized tables.

Customers

```
CREATE PROCEDURE [gold].[Refresh_Customers_Materialized_View]
AS
BEGIN
    -- 1. Empty the existing physical table
    TRUNCATE TABLE [gold].[customers_materialized_table];

    -- 2. Populate it with fresh data from the view
    INSERT INTO [gold].[customers_materialized_table]
    SELECT * FROM [gold].[vw_customers];
END;

```

Policies

```
CREATE PROCEDURE [gold].[Refresh_Policies_Materialized_View]
AS
BEGIN
    -- 1. Empty the existing physical table
    TRUNCATE TABLE [gold].[policies_materialized_table];

    -- 2. Populate it with fresh data from the view
    INSERT INTO [gold].[policies_materialized_table]
    SELECT * FROM [gold].[vw_policies];
END;

```

Create a master pipeline and connect the Fabric Items as shown below

![ALT](complete_pipeline.png)

## Reporting and Analyses

Create a semantic model for our Warehouse tables.

![ALT](semantic_model.png)

Build the following relationship in the semantic model

![ALT](semantic_model_rel.png)

A Power BI Report

![ALT](report.png)

**Brief policy analysis**

What happen?

![ALT](policies_by_month.png)

> Policy Sales — December 2022 Peak Performance

- December 2022 was the strongest sales month on record, with 8 policies sold — the highest single-month volume in the dataset

Why did it happen?

![ALT](Policy_by_type.png)

![ALT](policies_by_channel.png)

![ALT](broker_sales.png)

- Life policies accounted for half of all December sales, with 4 of the 8 policies (50%) — suggesting strong seasonal demand or a targeted sales push in that period
- The broker channel drove 4 of the 8 sales (50%), confirming brokers as a key contributor to the December peak
- Breaking down broker activity, brokers primarily sold Life policies — 3 of their 4 sales (75%) were Life, with 1 Auto policy making up the remainder
- This means 3 of the 4 Life policies sold in December came through the broker channel, indicating a strong and specific broker capability in Life products that is worth developing further

What should we do?

- Identify the specific brokers responsible and understand what drove their December performance
- Introduce a structured Life policy broker programme — dedicated training, competitive commission tiers, and marketing support focused specifically on Life products
- Set a formal Life policy sales target for brokers across all months, not just December
- Was there a broker incentive or promotion running in December 2022?
- Life policy purchases often spike in Q4 due to year-end financial planning and tax considerations — if this is a seasonal pattern, build a dedicated Q4 broker campaign around it
- Compare December 2022 broker activity to other months — if brokers are significantly underperforming the rest of the year, the gap represents recoverable revenue
- Assess whether brokers are equipped and incentivised to sell Auto policies — do they have the product knowledge and commission structure to make it worthwhile?
- Consider a targeted broker Auto pilot programme with a small group of high-performing brokers to test whether Auto sales can be scaled through the channel
- Diversify the Q4 sales strategy — invest in Health and Auto promotions alongside Life to broaden the product mix during the peak period
- Develop non-broker channels — direct and digital in particular — so that strong months are not dependent on broker activity alone

## Continuous Integration Continuous Delivery (CICD)

Coming soon
