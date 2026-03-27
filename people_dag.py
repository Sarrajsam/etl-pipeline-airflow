import datetime as dt
import csv
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# a python function that will clean / transform the data we are using
# this function is called / invoked from one of the operators in our DAG further below
def TransformData():
    # open a file on disk into which the transformed data will be written
    OutputFile = open('/tmp/people.txt', 'w')

    # write a header row into the output file for the comma-separated data we will be creating
    OutputFile.write("First,Last,Email\n")

    # open the input file that contains the original / raw data
    with open('/tmp/people.csv', encoding='utf-8-sig', newline='') as csvfile:
        # convert this file to a DictReaader - aids reading the rows in the data
        reader = csv.DictReader(csvfile)

        # for each row in the data
        for row in reader:
            # get the 'Name' field from the row, e.g., it will be something like "Ramsay, Craig"
            # use the 'split' command to separate the name into two items: "Ramsay" and "Craig"
            name = row['Name'].split(", ")

            # get the last name and first name from the data
            last = name[0]
            first = name[1]

            # create the line of data to write to the output file being the firstname, lastname, email
            OutLine = first + "," + last + "," + row['Email'] + "\n"

            # display the line for logging purposes - not required
            print(OutLine)

            # write the line to the output file
            OutputFile.write(OutLine)

# create the DAG object - required for Airflow
my_dag = DAG(                                                     
   dag_id="Craigs_DAG_Example",         # give the DAG a name. NOTE: I think underscores are required instead of spaces, e.g., "Craigs_DAG" versus "Craigs DAG"                       
   schedule_interval="@daily",          # how often to schedule the task - not as relevant for us, we will probably run this once                                     
   start_date=dt.datetime(2025, 1, 1), 
   catchup=False,
)

# define a Bash Operator that will download the data we want to work with into a local file on disk - this will be the first step / task in our workflow
download_data = BashOperator(                              
   task_id="download_data",             # make sure the DAG has a unique ID                               
   bash_command="curl -o /tmp/people.csv -L 'https://url/of/your/file/here'",
   dag=my_dag,                          # tell airflow which DAG this operator belongs to
)

# general form of a Bash Operator is:
#
# name_you_want_to_give_your_operator = BashOperator(
#                                           task_id="name_you_want_for_your_task_id",
#                                           bash_command="put your command in here",
#                                           dag=name_of_your_DAG_object_above,
#                                           )


# define a Python Operator that will transform the downloaded data into its intended output file - this will be the second step / task in our workflow
# this operator will invoke the 'TransformData' function which is shown further up this file
transform = PythonOperator(
   task_id="transform",
   python_callable=TransformData,       # the name of the function you want to be called when this operator is invoked
   dag=my_dag,
)

# general form of a Python Operator is:
#
# name_you_want_to_give_your_operator = PythonOperator(
#                                           task_id="name_you_want_for_your_task_id",
#                                           python_callable=name_of_the_function_you_want_to_call,
#                                           dag=name_of_your_DAG_object_above,
#                                           )

# define a Bash Operator that will copy the file that contains the transformed data from its temporary location here in Airflow over to another location on disk
# where it can be accessed later for any further processing - this will be the third step / task in our workflow
copy=BashOperator(
    task_id="copy",
    bash_command="cp /tmp/people.txt ~/airflow/dags/people.txt",
    dag=my_dag,
)

# define your workflow, e.g., show the sequence in which the operators should be performed using the '>>' operator, with the names of the operators that make up this workflow
download_data >> transform >> copy
    
