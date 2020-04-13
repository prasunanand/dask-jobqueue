import logging

import dask

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters

import boto3

logger = logging.getLogger(__name__)


class AWSBatchJob(Job):
	# submit_command = "submitjob"
	# cancel_command = "canceljob"
	config_name = "awsbatch"

	def __init__(self,
		client=None,
		account=None,
		aws_region=None,
		scheduler=None,
		name=None,
		queue=None,
		project=None,
		resource_spec=None,
		walltime=None,
		job_extra=None,
		config_name=None,
		**kwargs):

		super().__init__(
			scheduler=scheduler, name=name, config_name=config_name, **kwargs
		)

		if queue is None:
			queue = dask.config.get("jobqueue.%s.queue" % self.config_name)
		if project is None:
			project = dask.config.get("jobqueue.%s.project" % self.config_name)
		if walltime is None:
			walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
		if job_cpu is None:
			job_cpu = dask.config.get("jobqueue.%s.job-cpu" % self.config_name)
		if job_mem is None:
			job_mem = dask.config.get("jobqueue.%s.job-mem" % self.config_name)
		if job_extra is None:
			job_extra = dask.config.get("jobqueue.%s.job-extra" % self.config_name)


		self.client = client or boto3.client('batch')
		self.account = account or boto3.client('sts').get_caller_identity().get('Account')
		self.region = aws_region or boto3.session.Session().region_name


		self.job_header = header_template % config

		logger.debug("Job script: \n %s" % self.job_script())


	def register_job(self, container_name: str, s3uri_destination: str, job_def_name: str, ncpus: int, memoryInMB):
		"""
		Registers a job with aws batch.
		:param s3uri_destination: the name of the s3 bucket that will hold the data
		:param container_name: The name of the container to use e.g 324346001917.dkr.ecr.us-east-2.amazonaws.com/awscomprehend-sentiment-demo:latest
		"""
		role_name = "AWSBatchECSRole_{}".format(job_def_name)
		logger = logging.getLogger(__name__)

		##This is mandatory for aws batch
		assume_role_policy = {
			"Version": "2012-10-17",
			"Statement": [
				{
					"Sid": "",
					"Effect": "Allow",
					"Principal": {
						"Service": "ecs-tasks.amazonaws.com"
					},
					"Action": "sts:AssumeRole"
				}
			]
		}

		access_policy = create_access_policy(s3uri_destination)

		managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"]

		create_role(role_name, assume_role_policy, access_policy, managed_policy_arns)

		job_definition = get_job_definition(self.account, self.region, container_name, job_def_name, s3uri_destination,
											memoryInMB, ncpus,
											role_name)

		logger.info(
			"Creating a job with parameters \n {}".format(json.dumps(job_definition, sort_keys=False, indent=4)))
		response = self.client.register_job_definition(**job_definition)
		return response

	def submitjob():
		response = client.submit_job(
			jobName='string',
			jobQueue='string',
			arrayProperties={
				'size': 123
			},
			dependsOn=[
				{
					'jobId': 'string',
					'type': 'N_TO_N'|'SEQUENTIAL'
				},
			],
			jobDefinition='string',
			parameters={
				'string': 'string'
			},
			containerOverrides={
				'vcpus': 123,
				'memory': 123,
				'command': [
					'string',
				],
				'instanceType': 'string',
				'environment': [
					{
						'name': 'string',
						'value': 'string'
					},
				],
				'resourceRequirements': [
					{
						'value': 'string',
						'type': 'GPU'
					},
				]
			},
			nodeOverrides={
				'numNodes': 123,
				'nodePropertyOverrides': [
					{
						'targetNodes': 'string',
						'containerOverrides': {
							'vcpus': 123,
							'memory': 123,
							'command': [
								'string',
							],
							'instanceType': 'string',
							'environment': [
								{
									'name': 'string',
									'value': 'string'
								},
							],
							'resourceRequirements': [
								{
									'value': 'string',
									'type': 'GPU'
								},
							]
						}
					},
				]
			},
			retryStrategy={
				'attempts': 123
			},
			timeout={
				'attemptDurationSeconds': 123
			}
		)
		return response

	def canceljob():
		response = client.cancel_job(
			jobId='string',
			reason='string'
		)
		return response


class AWSBatchCluster(JobQueueCluster):
	__doc__ = """ Launch Dask on an AWS Batch cluster

	.. note::
		If you want a specific amount of RAM, both ``memory`` and ``resource_spec``
		must be specified. The exact syntax of ``resource_spec`` is defined by your
		GridEngine system administrator. The amount of ``memory`` requested should
		match the ``resource_spec``, so that Dask's memory management system can
		perform accurately.

	Parameters
	----------
	queue : str
		Destination queue for each worker job. Passed to `#$ -q` option.
	project : str
		Accounting string associated with each worker job. Passed to `#$ -A` option.
	{job}
	{cluster}
	resource_spec : str
		Request resources and specify job placement. Passed to `#$ -l` option.
	walltime : str
		Walltime for each worker job.
	job_extra : list
		List of other AWS Batch options, for example -w e. Each option will be
		prepended with the #$ prefix.

	Examples
	--------
	>>> from dask_jobqueue import SGECluster
	>>> cluster = AWSBatchCluster(
	...     queue='regular',
	...     project="myproj",
	...     cores=24,
	...     memory="500 GB"
	... )
	>>> cluster.scale(jobs=10)  # ask for 10 jobs

	>>> from dask.distributed import Client
	>>> client = Client(cluster)

	This also works with adaptive clusters.  This automatically launches and kill workers based on load.

	>>> cluster.adapt(maximum_jobs=20)
	""".format(
		job=job_parameters, cluster=cluster_parameters
	)
	print("Init AWSBatchCluster")
	job_cls = AWSBatchJob
	config_name = "awsbatch"
