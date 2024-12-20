from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import timedelta

# DAG default arguments
default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Retry delay if task fails
}

# Define the DAG
dag = DAG(
    'kaggle_scraping',  # DAG Name
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    max_active_runs=1,  # Optional: Limit the number of parallel running DAGs
)

hadoop_version = BashOperator(
    task_id="check_hadoop",
    bash_command="echo $PATH",
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HADOOP_HOME": "/opt/hadoop",
    },
)

# Step 1: Scraping the skin cancer data
scrape_data = BashOperator(
    task_id='scrape_data',
    bash_command='python3 /home/toto/script.py',  # Replace with your actual scraping script
    dag=dag,
)

# Step 2: Uploading scraped data to HDFS
upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command='hadoop fs -put /home/toto/downloaded_datasets /user/airflow',  # Update the local file and HDFS path
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HADOOP_HOME": "/opt/hadoop",
        "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64/"
    },
    dag=dag,
)

# Step 3: Deleting the scraped data after uploading it to HDFS
delete_local_data = BashOperator(
    task_id='delete_local_data',
    bash_command='rm -rf /home/toto/downloaded_datasets',  # Command to delete local data
    dag=dag,
)



run_spark_job = SSHOperator(
    task_id='run_spark_job',
    ssh_conn_id='hadoop_ssh_connection',  # Use the connection you set up
    command='export HADOOP_CONF_DIR=/home/hduser/hadoop-3.3.6/etc/hadoop && \
        export YARN_CONF_DIR=/home/hduser/hadoop-3.3.6/etc/hadoop && \
        /opt/spark/bin/spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --conf spark.yarn.submit.waitAppCompletion=true \
        --class com.example.App \
        /home/hduser/spark-job/target/spark-job-1.0-SNAPSHOT-jar-with-dependencies.jar',
    execution_timeout=timedelta(seconds=7200),
    dag=dag,
)

# Set task dependencies
hadoop_version >> scrape_data >> upload_to_hdfs >> delete_local_data >> run_spark_job