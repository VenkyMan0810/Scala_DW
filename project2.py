from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

with DAG(
    "project2-workflow",
    start_date=datetime(2015, 12, 1),
    schedule_interval=None,
    # TODO Running the solution each time in the configuration of Apache Airflow script correct the paths in parameters jars_home and bucket_home
    params={
      "jars_home": Param("/home/<nazwa-uzytkownika>/airflow/dags/project_files", type="string"),
      "bucket_name": Param("<nazwa-zasobnika>", type="string"),
    },
    render_template_as_native_obj=True
) as dag:

  clean_output_dir = BashOperator(
    task_id="clean_output_dir",
    bash_command="""if $(hadoop fs -test -d /tmp/delta/) ; then hadoop fs -rm -f -r /tmp/delta/; fi""",
  )

  load_fact_table = BashOperator(
    task_id="load_fact_table",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 8 \
                                  --driver-memory 64g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  {{ params.jars_home }}/fact.jar {{ params.bucket_name }}"""
  )

  load_road_dim = BashOperator(
    task_id="load_road_dim",
    bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                  --class UKTrafficAnalysis \
                                  --master yarn \
                                  --num-executors 8 \
                                  --driver-memory 64g \
                                  --executor-memory 2g \
                                  --executor-cores 2 \
                                  {{ params.jars_home }}/dim_road.jar {{ params.bucket_name }}"""
  )

  load_weather_dim = BashOperator(
      task_id="load_weather_dim",
      bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                    --class UKTrafficAnalysis \
                                    --master yarn \
                                    --num-executors 8 \
                                    --driver-memory 64g \
                                    --executor-memory 2g \
                                    --executor-cores 2 \
                                    {{ params.jars_home }}/dim_weather.jar {{ params.bucket_name }}"""
    )

  load_directions_dim = BashOperator(
      task_id="load_directions_dim",
      bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                    --class UKTrafficAnalysis \
                                    --master yarn \
                                    --num-executors 8 \
                                    --driver-memory 64g \
                                    --executor-memory 2g \
                                    --executor-cores 2 \
                                    {{ params.jars_home }}/dim_directions.jar {{ params.bucket_name }}"""
    )

  load_regions_dim = BashOperator(
      task_id="load_regions_dim",
      bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                    --class UKTrafficAnalysis \
                                    --master yarn \
                                    --num-executors 8 \
                                    --driver-memory 64g \
                                    --executor-memory 2g \
                                    --executor-cores 2 \
                                    {{ params.jars_home }}/dim_regions.jar {{ params.bucket_name }}"""
    )

  load_time_dim = BashOperator(
      task_id="load_time_dim",
      bash_command=""" spark-submit --packages io.delta:delta-core_2.12:2.1.0 \
                                    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                                    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                                    --class UKTrafficAnalysis \
                                    --master yarn \
                                    --num-executors 8 \
                                    --driver-memory 64g \
                                    --executor-memory 2g \
                                    --executor-cores 2 \
                                    {{ params.jars_home }}/dim_time.jar {{ params.bucket_name }}"""
  )


clean_output_dir >> load_regions_dim
load_regions_dim >> load_weather_dim
load_weather_dim >> load_road_dim
load_road_dim >> load_directions_dim
load_directions_dim >> load_time_dim
load_time_dim >> load_fact_table
