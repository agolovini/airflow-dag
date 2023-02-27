# Structure of DAG

### Block of imports

```Python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # for task in python
from datetime import datetime
```

[list of operators](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html)

### Block code
#### Here you write you functions:
```python
def foo1():    
def foo2():
def foo3():
```

### Block iitialization
#### Setting parameters in DAG: 
```python
default_args = {
    'owner': 'your_name', # owner operations
    'depends_on_past': False, # Dependency on previous runsv

    'schedule_interval': '0 12 * * *' # cron expressions, also available '@daily', '@weekly'
    #'schedule_interval': '@daily' variable airflow
    #'schedule_interval': timedelta() parameter timedelta

    'retries': 1, # numbers of attemps to run the DAG
    'retry_delay': timedelta(minutes=5), # interval between runs
    'email': '', # email for notifications
    'email_on_failure': '', # email for notifications about errors
    'email_on_retry': '', # email for notifications about restart

    'retry_exponential_backoff': '', # for exponential time between runs
    'max_retry_delay': '', # maximum delay before re-run

    'start_date': '', # date of starting DAG
    'end_date': '', # date of ending DAG

    'on_failure_callback': '', # Launch function if DAG collapsed
    'on_success_callback': '', # Launch function if DAG succeed
    'on_retry_callback': '', #  Launch function if DAG goes on second launch
    'on_execute_callback': '', # Launch function if DAG starting to execute
     # documentation
    'doc': '',
    'doc_md': '',
    'doc_rst': '',
    'doc_json': '',
    'doc_yaml': ''
}
dag = DAG('DAG_name', default_args=default_args)
```

[More about DAG parameters](https://airflow.apache.org/docs/apache-airflow/2.1.2/_api/airflow/models/index.html)

#### Initialize tasks: 
```python
t1 = PythonOperator(task_id='foo1', # name of task
                    python_callable=foo1, # name of function
                    dag=dag) # parameters of DAG

t2 = PythonOperator(task_id='foo2', 
                    python_callable=foo2, 
                    dag=dag) 

t3 = PythonOperator(task_id='foo3', 
                    python_callable=foo3, 
                    dag=dag) 
```

### Block of logic
#### Setting up the execution logic
```python
# Python Operators
t1 >> t2 >> t3 
```
```python
# Methods of task
t1.set_downstream(t2)
t2.set_downstream(t3)
```

For parallel task execution, Python operators are used in the form of
```python
 A >> [B, C] >> D
```
or dependencies are specified using task methods
```python
A.set_downstream(B)
A.set_downstream(C)
B.set_downstream(D)
C.set_downstream(D)
```
Tasks B and C will be executed in parallel, and D will be executed only after successful completion of B and C.