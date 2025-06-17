import airflow.utils.dates
from pathlib import Path
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BASE_DIR = Path(__file__).resolve().parent.parent

default_args= {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

with DAG(
    'spark-batch-job',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    bronze_layer_load = SparkSubmitOperator(
        task_id="bronze_layer_load",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "bronze_dimension_fact_load.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        jars=str(BASE_DIR / "mysql-connector-j-8.0.33.jar"),
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    silver_layer_dimension_transform = SparkSubmitOperator(
        task_id="silver_layer_dimension_transform",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "silver_dimensions.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316",
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    silver_layer_fact_transform = SparkSubmitOperator(
        task_id="silver_layer_fact_transform",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "silver_facts.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316",
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    gold_layer_dim_payment_scd2 = SparkSubmitOperator(
        task_id="gold_layer_dim_payment_scd2",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "gold_dim_payment.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316,io.delta:delta-spark_2.12:3.0.0",
        repositories='https://repo1.maven.org/maven2/',        
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    gold_layer_dim_stores_scd2 = SparkSubmitOperator(
        task_id="gold_layer_dim_stores_scd2",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "gold_dim_stores.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316,io.delta:delta-spark_2.12:3.0.0",
        repositories='https://repo1.maven.org/maven2/',
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    gold_layer_dim_products_scd2 = SparkSubmitOperator(
        task_id="gold_layer_dim_products_scd2",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "gold_dim_products.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316,io.delta:delta-spark_2.12:3.0.0",
        repositories='https://repo1.maven.org/maven2/',
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    gold_layer_fact_orders = SparkSubmitOperator(
        task_id="gold_layer_fact_orders",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "gold_fact_orders.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316,io.delta:delta-spark_2.12:3.0.0",
        repositories='https://repo1.maven.org/maven2/',
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    bronze_data_quality_check = SparkSubmitOperator(
        task_id="bronze_data_quality_check",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "data_quality" / "bronze_validation.py"),
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316",
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

    silver_data_quality_check = SparkSubmitOperator(
        task_id="silver_data_quality_check",
        conn_id="spark",
        application=str(BASE_DIR / "scripts" / "data_quality" / "silver_validation.py"),
        env_vars={'SPARK_VERSION': '3.5.1'},
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.316,com.amazon.deequ:deequ:2.0.7-spark-3.5",
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console",
            "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=ERROR,console"
        }
    )

# Bronze → Bronze Quality Check
bronze_layer_load >> bronze_data_quality_check

# Bronze Quality Check → Silver Layer (Dimensions + Fact)
bronze_data_quality_check >> [silver_layer_dimension_transform, silver_layer_fact_transform]

# Silver Layers → Silver Quality Check
[silver_layer_dimension_transform, silver_layer_fact_transform] >> silver_data_quality_check

# Silver Quality Check → Gold Dimensions
silver_data_quality_check >> [
    gold_layer_dim_payment_scd2,
    gold_layer_dim_stores_scd2,
    gold_layer_dim_products_scd2
]

# Gold Dimensions → Gold Fact
[
    gold_layer_dim_payment_scd2,
    gold_layer_dim_stores_scd2,
    gold_layer_dim_products_scd2
] >> gold_layer_fact_orders