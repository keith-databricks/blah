# Databricks notebook source
# MAGIC %md # Steps to initialize the cluster for the Kernel Gateway

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 0. "run all" this notebook. This will write a init script to dbfs
# MAGIC 0. on the clusters page, add the init script `dbfs:/databricks/jupyter/kernel_gateway_init.sh`
# MAGIC 0. restart cluster (to run init script)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Init Script

# COMMAND ----------

kernel_gateway_init = """
#!/bin/bash

KG_PORT=4010
KERNEL_NAME=${DB_CLUSTER_ID}___python3
DB_MAJOR_VERSION=${DATABRICKS_RUNTIME_VERSION:0:1}
DATABRICKS_PYTHON_BASE=/databricks/python
if [ "$DB_MAJOR_VERSION" -eq "5" ]; then
    DATABRICKS_PYTHON_BASE=/databricks/python3
fi
DATABRICKS_PIP=${DATABRICKS_PYTHON_BASE}/bin/pip
KERNEL_PATH=${DATABRICKS_PYTHON_BASE}/share/jupyter/kernels

# Jupyter is only supported on runtime versions 5.x and 6.x.
if [ "$DB_MAJOR_VERSION" -ne "6" ] && [ "$DB_MAJOR_VERSION" -ne "5" ]; then
    exit 1
fi

if [ -n "$CONDA_PYTHON_EXE" ]; then 
    conda install jupyter
fi

# Install DBR dependencies
if [ -z "$MLR_PYTHONPATH" ]; then
    ${DATABRICKS_PIP} install --no-dependencies \
        attrs==19.1.0 \
        backcall==0.1.0 \
        bleach==3.1.0 \
        defusedxml==0.5.0 \
        entrypoints==0.3 \
        ipykernel==5.1.0 \
        ipython==6.4.0 \
        jedi==0.13.3 \
        jsonschema==3.0.1 \
        jupyter-client==5.2.3 \
        jupyter-core==4.4.0 \
        mistune==0.8.4 \
        nbconvert==5.4.1 \
        nbformat==4.4.0 \
        notebook==5.7.5 \
        pandocfilters==1.4.2 \
        parso==0.3.4 \
        prometheus-client==0.6.0 \
        prompt-toolkit==1.0.15 \
        pyrsistent==0.14.11 \
        pyzmq==18.0.1 \
        Send2Trash==1.5.0 \
        simplegeneric==0.8.1 \
        testpath==0.4.2 \
        terminado==0.8.1 \
        tornado==6.0.1 \
        webencodings==0.5.1
fi

# Install kernel gateway
${DATABRICKS_PIP} install --no-dependencies \
    jupyter_kernel_gateway==2.3.0 \
    notebook==5.7.8 \
    ipykernel==5.1.1 \
    prometheus-client==0.7.1 \
    terminado==0.8.2 \
    Send2Trash==1.5.0



# Install ipywidgets
# DBR 5.3, 5.2, 5.1, 5.0. MLR 5.2, 5.1, 5.0 tested for ipywidgets compatibility
# NOTE: ipywidgets v7.5.0 incompatible with jupyterlab ipywidgets frontend extension v0.38.1 and
# jupyterlab v0.35.4.
${DATABRICKS_PIP} install --no-dependencies \
    ipywidgets==7.5.0 \
    decorator==4.0.10 \
    Jinja2==2.10.1 \
    MarkupSafe==0.23 \
    pexpect==4.7.0 \
    pickleshare==0.7.5 \
    ptyprocess==0.6.0 \
    Pygments==2.4.2 \
    python-dateutil==2.8.0 \
    six==1.12.0 \
    testpath==0.4.2 \
    traitlets==4.3.2 \
    wcwidth==0.1.7 \
    widgetsnbextension==3.5.0



# Installing package needed for ipywidgets only in DBR since installation of package in MLRuntime
# causes an error and package already exists in MLRuntime
if [ -z "$MLR_PYTHONPATH" ]; then
    ${DATABRICKS_PIP} install --no-dependencies \
        ipython-genutils==0.1.0
fi

# Set up ipython profile for Jupyter with a custom seed script
${DATABRICKS_PYTHON_BASE}/bin/ipython profile create jupyter
cat >  ~/.ipython/profile_jupyter/startup/01_setup_context.py <<EOF
import os
import re
import inspect
import sys
from IPython.display import HTML, display

def get_cloud_resource_dir():
    '''This is used for DBR 4 see PythonLocalDriver.py'''
    dirs = list(filter(lambda x: re.search('[0-9]{13}-[0-9]', x), os.listdir('/tmp')))
    if len(dirs) >= 1:
        return '/tmp/%s' % dirs[0]
    return None

def has_parameter(fn, parameter_name):
    '''checks whether the function has provided parameter'''
    return inspect.signature(fn).parameters.get(parameter_name) != None
    
def _dbutils(sc, sqlContext, gateway):
    '''return dbutils, with unsupported operations stubbed'''    
    from dbutils import DBUtils
    shell = get_ipython()
    shell.sc = sc
    shell.sqlContext = sqlContext
    shell.displayHTML = lambda html: display(HTML(html))
    dbutils = DBUtils(shell, gateway.entry_point)
    dbutils.notebook = 'Not supported in hosted Jupyter'
    dbutils.library = 'Not supported in hosted Jupyter'
    dbutils.widgets = 'Not supported in hosted Jupyter'
    return dbutils

os.environ['PYSPARK_PYTHON']='${DATABRICKS_PYTHON_BASE}/bin/python3'

sys.path.append("/databricks/spark/python")
sys.path.append("/databricks/jars/spark--driver--spark--resources-resources.jar")
sys.path.insert(0, "/databricks/spark/python/lib/py4j-0.10.7-src.zip") # this is required for dbr 5

cloud_resource_dir = get_cloud_resource_dir()
if cloud_resource_dir:
    sys.path.insert(0, cloud_resource_dir)

from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql import functions

from PythonShell import get_existing_gateway, RemoteContext

GW_PORT = int(os.environ.get('GATEWAY_SERVER_PORT', -1))

if has_parameter(get_existing_gateway, 'pinned_mode'):
    GW_AUTH_TOKEN = os.environ.get('GATEWAY_SERVER_AUTH_TOKEN', 'NO_TOKEN')
    # pinned mode is off by default, see "spark.databricks.pyspark.py4j.pinnedThread"
    gateway = get_existing_gateway(GW_PORT, True, GW_AUTH_TOKEN, False)
elif has_parameter(get_existing_gateway, 'auth_token'):
    GW_AUTH_TOKEN = os.environ.get('GATEWAY_SERVER_AUTH_TOKEN', 'NO_TOKEN')
    gateway = get_existing_gateway(GW_PORT, True, GW_AUTH_TOKEN)
else:
    gateway = get_existing_gateway(GW_PORT, auto_convert=True)

kernel_working_dir = os.environ.get('KERNEL_WORKING_DIR', '')
if kernel_working_dir:
    try:
        os.chdir(kernel_working_dir)
    except Exception as e:
        print("failed to change to kernel working dir: " + str(e))
        pass
else:
    print("env.KERNEL_WORKING_DIR is missing.")
    pass

# set up global variables
conf = SparkConf(_jconf=gateway.entry_point.getSparkConf())
sc = RemoteContext(gateway=gateway, conf=conf)
sqlContext = HiveContext(sc, gateway.entry_point.getSQLContext())
spark = sqlContext.sparkSession
dbutils = _dbutils(sc, sqlContext, gateway)
displayHTML = lambda html: display(HTML(html))

EOF

# This python script must be executed after context is set up
# iPython executes files in startup folder in lexicographic order
# Therefore, use numbers to enforce required order
cat >  ~/.ipython/profile_jupyter/startup/02_progress_reporter.py <<EOF
import threading
import requests
import json
import time, datetime
import uuid
import ipywidgets as widgets
import os, re
import ipykernel
from ipykernel.comm import Comm

CLUSTER_NAME = os.environ.get('CLUSTER_NAME', 'N/A')
USER_NAME = os.environ.get('USER_NAME', 'N/A')

kernel_id = re.search('kernel-(.*).json', ipykernel.connect.get_connection_file()).group(1)

# magic numbers
# 0.5 seconds is the interval that chauffeur poller uses
UPDATE_SLEEP_TIME_SEC = 0.5
# 0.2  set to be long enough to guarantee to an extent for poller thread's execution of setJobGroup
# to be during main thread's sleep, but short enough not to delay user's command execution too much
WORKAROUND_SLEEP_TIME_SEC = 0.2

def _init_progress_reporter():
    class Progress(object):
        '''Track and display command progress of Spark jobs executed by user in jupyter notebook cell

        This class uses the SparkContext object as an entrypoint to obtain information about Spark jobs
        executed by a user. This information is then displayed to the user using ipywidgets.
        A poller thread created in the pre-run cell hook fetches spark job progress information every
        UPDATE_SLEEP_TIME_SEC seconds and updates the display as the main thread executes user's commands.
        Two dictionaries are created, one to store progress status data and one to store widgets.

        Args:
            sc (SparkContext): entrypoint to get spark job progress data

        Attributes:
            sc (SparkContext): entrypoint to get spark job progress data
            tracker (SparkStatusTracker): access point to API to monitor job and stage progress
            startTime (float): timer to track total execution time
            progressStatus (dict): map to store command progress data for each stage of each job
            container (ipwidget.Accordion): container widget that contains all children widgets
            childrenWidgets (dict): map to store widgets created for each stage of each job so they can be updated
        '''
        def __init__(self, sc):
            self.sc = sc
            self.tracker = sc.statusTracker()
            self.startTime = None
            self.progressStatus = None
            self.container = None
            self.childrenWidgets = None

        def _updateStageWidget(self, jobId, stageId, numCompletedTasks, numTasks, numActiveTasks, progress):
            '''Update existing widget to show new data about a stage

            Args:
                jobId (int): jobId of stage being updated
                stageId (int): stageId of stage being updated
                numCompletedTasks (int): number of completed tasks in stage
                numTasks (int): total number of tasks in stage
                numActiveTasks (int): number of active tasks in stage
                progress (int): int(100 * numCompletedTasks / numTasks),
            '''
            assert (jobId in self.childrenWidgets), 'jobId {} -> jobWidget mapping does not exist in childrenWidgets'.format(jobId)

            intProgress, tasksLabel = self.childrenWidgets[jobId]['stageWidgetMap'][stageId].children
            intProgress.value = progress
            label = '{}/{} ({} running)'.format(numCompletedTasks, numTasks, numActiveTasks)
            tasksLabel.value = label
            self.childrenWidgets[jobId]['stageWidgetMap'][stageId].children = (intProgress, tasksLabel)

        def _addStageWidget(self, jobId, stageId, numCompletedTasks, numTasks, numActiveTasks, progress):
            '''Add new widget to display tasks completion progress of a stage

            Args:
                jobId (int): jobId of stage being updated
                stageId (int): stageId of stage being updated
                numCompletedTasks (int): number of completed tasks in stage
                numTasks (int): total number of tasks in stage
                numActiveTasks (int): number of active tasks in stage
                progress (int): int(100 * numCompletedTasks / numTasks),
            '''
            assert (jobId in self.childrenWidgets), 'jobId {} -> jobWidget mapping does not exist in childrenWidgets'.format(jobId)

            label = '{}/{} ({} running)'.format(numCompletedTasks, numTasks, numActiveTasks)
            tasksLabel = widgets.Label(label)
            intProgress = widgets.IntProgress(
                    value=progress,
                    min=0,
                    max=100,
                    step=1,
                    description='Stage {}:'.format(stageId),
                    bar_style='',
                    orientation='horizontal'
                )
            items = [intProgress, tasksLabel]
            stageWidget = widgets.HBox(items)

            self.childrenWidgets[jobId]['stageWidgetMap'][stageId] = stageWidget
            jobWidgetBox = self.childrenWidgets[jobId]['jobWidget'].children[0]
            jobWidgetBox.children = jobWidgetBox.children + (stageWidget,)

        def _addJobWidget(self, jobId):
            '''Add new widget to show execution progress of job with jobId

            Args:
                jobId (int): jobId of job being added
            '''
            jobWidget = widgets.Accordion(children=[widgets.VBox()], selected_index=None)
            numStages = len(self.tracker.getJobInfo(jobId).stageIds)
            jobWidget.set_title(0, 'Job {} ({} stages)'.format(jobId, numStages))
            vbox = self.container.children[0]
            vbox.children = vbox.children + (jobWidget,)
            self.childrenWidgets[jobId] = {'stageWidgetMap': {}, 'jobWidget': jobWidget}

        def _updateWidgets(self):
            '''Go through updated data in progressStatus map to display latest job progress status
            '''
            jobCount = len(self.progressStatus)
            self.container.set_title(0, '({}) Spark Jobs'.format(jobCount))

            for jobId, value in self.progressStatus.items():
                # there exists a spark job, so display progressWidget
                self.container.layout = widgets.Layout(display='display')

                if jobId not in self.childrenWidgets:
                    self._addJobWidget(jobId)

                for stageId, infoMap in value.items():
                    if stageId not in self.childrenWidgets[jobId]['stageWidgetMap']:
                        self._addStageWidget(jobId,
                                             stageId,
                                             infoMap['numCompletedTasks'],
                                             infoMap['numTasks'],
                                             infoMap['numActiveTasks'],
                                             infoMap['progress'])
                    else:
                        self._updateStageWidget(jobId,
                                                stageId,
                                                infoMap['numCompletedTasks'],
                                                infoMap['numTasks'],
                                                infoMap['numActiveTasks'],
                                                infoMap['progress'])

        def _updateStatus(self, groupId):
            '''Use SparkContext object to obtain data about job execution progress and update progressStatus map

            Args:
                groupId (str): groupId of group of Spark jobs executed by user.
                    This groupId is generated in pre_run_cell and set in both main and poller threads
            '''
            for j in self.tracker.getJobIdsForGroup(groupId):
                if j not in self.progressStatus:
                    self.progressStatus[j] = {}
                jobInfo = self.tracker.getJobInfo(j)

                # sort stageIds so they appear in order
                stageIds = list(jobInfo.stageIds)
                stageIds.sort()

                for s in stageIds:
                    stageInfo = self.tracker.getStageInfo(s)
                    infoMap = {
                        'numCompletedTasks': stageInfo.numCompletedTasks,
                        'numTasks': stageInfo.numTasks,
                        'progress': int(100 * stageInfo.numCompletedTasks / stageInfo.numTasks),
                        'numActiveTasks': stageInfo.numActiveTasks
                    }
                    self.progressStatus[j][s] = infoMap

        def poller(self, groupId, barrier, description):
            '''Entrypoint for poller thread that obtains spark job status while main thread executes user code
            Update widgets displaying command progress every UPDATE_SLEEP_TIME_SEC seconds

            Args:
                groupId (str): groupId of group of Spark jobs executed by user.
                barrier (threading.Barrier): used to synchronize execution of main and poller threads
            '''
            barrier.wait() # synchronize main and poller threads

            # workaround pyspark bug mentioned in pre_run_cell
            time.sleep(WORKAROUND_SLEEP_TIME_SEC / 2)
            self.sc.setJobGroup(groupId, description)

            # ensure that spark jobs don't have scheduling conflict by giving each cell unique poolId
            self.sc.setLocalProperty("spark.scheduler.pool", groupId)

            # widget that encases all widgets
            self.container = widgets.Accordion(children=[widgets.VBox()], selected_index=None, layout=widgets.Layout(display='none'))

            # store command progress data for each stage of each job
            self.progressStatus = {}

            # store widgets created for each stage of each job so they can be updated
            self.childrenWidgets = {}

            display(self.container)

            # update status every UPDATE_SLEEP_TIME_SEC seconds
            while (self.running):
                time.sleep(UPDATE_SLEEP_TIME_SEC)
                self._updateStatus(groupId)
                self._updateWidgets()

            # last update after execution of cell terminates
            self._updateStatus(groupId)
            self._updateWidgets()

        def pre_run_cell(self, info):
            '''Function is executed before execution of every cell
            '''

            # Use comm to send a message from the kernel
            status_comm = Comm(target_name=kernel_id, data={'type': 'status'})
            status_comm.send({
                'type': 'running'
                })

            groupId = str(uuid.uuid1())
            # timeout of 5s covers difference in time for two threads to reach wait
            barrier = threading.Barrier(2, timeout=5)

            # get string for user's command to set as job description
            # trim to 50 characters following databricks implementation
            description = info.raw_cell
            if len(description) > 50:
                description = description[:47] + "..."

            # self.running must be set to True before poller thread starts for
            # poller thread to enter while loop that updates command progress status
            self.t = threading.Thread(target=self.poller, args=(groupId, barrier, description,))
            self.running = True
            self.t.start()

            self.sc.setJobGroup(groupId, description)
            self.sc.setLocalProperty("spark.scheduler.pool", groupId)

            # Workaround for JIRA issue SC-18993
            # JIRA issue for implementing workaround: PROD-18879
            # Workaround details:
            # setJobGroup attaches a groupId to all Spark jobs executed in main thread.
            # Concurrent execution of getJobIdsForGroup in poller thread and Spark job in
            # main thread leads to Py4J spawning a new Java thread to execute both commands.
            # Py4J does not guarantee 1-1 mapping between main and poller threads in our
            # python code with the two threads in JVM and main thread's jobGroupId is stored
            # in thread local storage on JVM side. There is therefore a possibility that Spark job
            # execution in main thread is sent to the second thread in JVM without jobGroupId set.

            # The workaround is to force main Java thread with jobGroupId set to sleep
            # and concurrently setJobGroupId in poller thread. This forces a second Java
            # thread with the same jobGroupId set to be created. So, whichever Java thread
            # a Spark job is sent to be executed, the job will be assigned the same
            # groupId allowing us to track its execution progress.

            # barrier used to minimize variability between time it takes for main
            # thread to get to sleep statement and time it takes for poller thread
            # to get to setJobGroup statement
            barrier.wait()
            # sleep JVM thread so poller thread can setJobGroup to second thread
            self.sc._jvm.java.lang.Thread.sleep(int(WORKAROUND_SLEEP_TIME_SEC * 1000))

            self.startTime = time.time()

        def post_run_cell(self, result):
            elapsedTime = time.time() - self.startTime

            now = datetime.datetime.now().strftime('%m/%d/%Y, %I:%M:%S %p UTC') # TODO: check if can get user's timezone
            message = 'Command took {:.2f} seconds -- by {} at {} on {}'.format(elapsedTime, USER_NAME, now, CLUSTER_NAME)

            status_comm = Comm(target_name=kernel_id, data={'type': 'status'})
            status_comm.send({
                'type': 'finished',
                'message': message
                })

            # necessary for poller thread to terminate
            self.running = False
            # cleanup resources and avoid next cell being executed before current cell's poller thread terminates
            self.t.join()
            
        def driver_keep_alive(self):
            payload = {
                "@class": "com.databricks.backend.common.rpc.InternalDriverBackendMessages$DriverExecutionStatus", 
                "activeSparkJobs": 1, 
                "activeJdbcCommands": 0
            }
            chauffeur_internal_host_port = 'localhost:7071'
            url = 'http://{}/?type="com.databricks.backend.common.rpc.InternalDriverBackendMessages$DriverExecutionStatus"'.format(chauffeur_internal_host_port)
            
            requests.post(url=url, json=json.dumps(payload))
            

    # Code for clearing of other registered callbacks is inherited from textual WIP
    # not sure if this is strictly necessary. Same with info and result arguments of pre_run_cel
    # and post_run_cell methods respectively
    ip = get_ipython()
    ip.events.callbacks['pre_run_cell'] = []
    ip.events.callbacks['post_run_cell'] = []
    progress = Progress(sc)
    ip.events.register('pre_run_cell', progress.pre_run_cell)
    ip.events.register('post_run_cell', progress.post_run_cell)
    ip.events.register('pre_run_cell', progress.driver_keep_alive)
    

_init_progress_reporter()
EOF

# Rename kernel to contain cluster id
mv ${KERNEL_PATH}/python3 ${KERNEL_PATH}/${KERNEL_NAME}

# Overwrite kernel.json to set the ipython profile to jupyter. This can't be done in the kernelspec manager
# on the notebook server, as the notebook server doesn't pass the kernelspec to the kernel gateway.
cat > ${KERNEL_PATH}/${KERNEL_NAME}/kernel.json <<EOF
{
 "argv": ["python", "-m", "ipykernel_launcher", "--profile=jupyter", "-f", "{connection_file}"],
 "display_name": "Python 3",
 "language": "python"
}
EOF

mkdir /databricks/jupyter

nohup ${DATABRICKS_PYTHON_BASE}/bin/jupyter kernelgateway --ip='' --port=${KG_PORT} --KernelGatewayApp.force_kernel_name="${KERNEL_NAME}" --JupyterWebsocketPersonality.env_whitelist='["GATEWAY_SERVER_PORT", "GATEWAY_SERVER_AUTH_TOKEN", "CLUSTER_NAME", "USER_NAME", "INITIAL_WORKING_DIR"]' &>> /databricks/jupyter/kg_nohup.log &
"""  # kernel_gateway_init_end

location = "/databricks/jupyter/kernel_gateway_init.sh"
dbutils.fs.mkdirs("dbfs:/databricks/jupyter/")
dbutils.fs.put(location, kernel_gateway_init, True)
