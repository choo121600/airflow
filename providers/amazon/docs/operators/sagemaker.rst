 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

================
Amazon SageMaker
================

`Amazon SageMaker <https://docs.aws.amazon.com/sagemaker>`__ is a fully managed
machine learning service. With Amazon SageMaker, data scientists and developers
can quickly build and train machine learning models, and then deploy them into a
production-ready hosted environment.

Airflow provides operators to create and interact with SageMaker Jobs and Pipelines.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:SageMakerProcessingOperator:

Create an Amazon SageMaker processing job
=========================================

To create an Amazon Sagemaker processing job to sanitize your dataset you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_processing]
    :end-before: [END howto_operator_sagemaker_processing]

.. _howto/operator:SageMakerTrainingOperator:

Create an Amazon SageMaker training job
=======================================

To create an Amazon Sagemaker training job you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_training]
    :end-before: [END howto_operator_sagemaker_training]

.. _howto/operator:SageMakerModelOperator:

Create an Amazon SageMaker model
================================

To create an Amazon Sagemaker model you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerModelOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_model]
    :end-before: [END howto_operator_sagemaker_model]

.. _howto/operator:SageMakerTuningOperator:

Start a hyperparameter tuning job
=================================

To start a hyperparameter tuning job for an Amazon Sagemaker model you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTuningOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_tuning]
    :end-before: [END howto_operator_sagemaker_tuning]

.. _howto/operator:SageMakerDeleteModelOperator:

Delete an Amazon SageMaker model
================================

To delete an Amazon Sagemaker model you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteModelOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_delete_model]
    :end-before: [END howto_operator_sagemaker_delete_model]

.. _howto/operator:SageMakerTransformOperator:

Create an Amazon SageMaker transform job
========================================

To create an Amazon Sagemaker transform job you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_transform]
    :end-before: [END howto_operator_sagemaker_transform]

.. _howto/operator:SageMakerEndpointConfigOperator:

Create an Amazon SageMaker endpoint config job
==============================================

To create an Amazon Sagemaker endpoint config job you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointConfigOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_endpoint_config]
    :end-before: [END howto_operator_sagemaker_endpoint_config]

.. _howto/operator:SageMakerEndpointOperator:

Create an Amazon SageMaker endpoint job
=======================================

To create an Amazon Sagemaker endpoint you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_endpoint]
    :end-before: [END howto_operator_sagemaker_endpoint]

.. _howto/operator:SageMakerStartPipelineOperator:

Start an Amazon SageMaker pipeline execution
============================================

To trigger an execution run for an already-defined Amazon Sagemaker pipeline, you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartPipelineOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_pipeline.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_start_pipeline]
    :end-before: [END howto_operator_sagemaker_start_pipeline]

.. _howto/operator:SageMakerStopPipelineOperator:

Stop an Amazon SageMaker pipeline execution
===========================================

To stop an Amazon Sagemaker pipeline execution that is currently running, you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerStopPipelineOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_pipeline.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_stop_pipeline]
    :end-before: [END howto_operator_sagemaker_stop_pipeline]

.. _howto/operator:SageMakerRegisterModelVersionOperator:

Register a Sagemaker Model Version
==================================

To register a model version, you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerRegisterModelVersionOperator`.
The result of executing this operator is a model package.
A model package is a reusable model artifacts abstraction that packages all ingredients necessary for inference.
It consists of an inference specification that defines the inference image to use along with a model weights location.
A model package group is a collection of model packages.
You can use this operator to add a new version and model package to the group for every DAG run.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_register]
    :end-before: [END howto_operator_sagemaker_register]

.. _howto/operator:SageMakerAutoMLOperator:

Launch an AutoML experiment
===========================

To launch an AutoML experiment, a.k.a. SageMaker Autopilot, you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerAutoMLOperator`.
An AutoML experiment will take some input data in CSV and the column it should learn to predict,
and train models on it without needing human supervision.
The output is placed in an S3 bucket, and automatically deployed if configured for it.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_auto_ml]
    :end-before: [END howto_operator_sagemaker_auto_ml]

.. _howto/operator:SageMakerCreateExperimentOperator:

Create an Experiment for later use
==================================

To create a SageMaker experiment, you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateExperimentOperator`.
This creates an experiment so that it's ready to be associated with processing, training and transform jobs.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_experiment]
    :end-before: [END howto_operator_sagemaker_experiment]

.. _howto/operator:SageMakerCreateNotebookOperator:

Create a SageMaker Notebook Instance
====================================

To create a SageMaker Notebook Instance , you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateNotebookOperator`.
This creates a SageMaker Notebook Instance ready to run Jupyter notebooks.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_notebook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_notebook_create]
    :end-before: [END howto_operator_sagemaker_notebook_create]

.. _howto/operator:SageMakerStopNotebookOperator:

Stop a SageMaker Notebook Instance
==================================

To terminate SageMaker Notebook Instance , you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerStopNotebookOperator`.
This terminates the ML compute instance and disconnects the ML storage volume.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_notebook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_notebook_stop]
    :end-before: [END howto_operator_sagemaker_notebook_stop]

.. _howto/operator:SageMakerStartNotebookOperator:

Start a SageMaker Notebook Instance
===================================

To launch a SageMaker Notebook Instance and re-attach an ML storage volume, you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartNotebookOperator`.
This launches a new ML compute instance with the latest version of the libraries and attached your ML storage volume.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_notebook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_notebook_start]
    :end-before: [END howto_operator_sagemaker_notebook_start]


.. _howto/operator:SageMakerDeleteNotebookOperator:

Delete a SageMaker Notebook Instance
====================================

To delete a SageMaker Notebook Instance, you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteNotebookOperator`.
This terminates the instance and deletes the ML storage volume and network interface associated with the instance. The instance must be stopped before it can be deleted.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_notebook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_notebook_delete]
    :end-before: [END howto_operator_sagemaker_notebook_delete]

Sensors
-------

.. _howto/sensor:SageMakerTrainingSensor:

Wait on an Amazon SageMaker training job state
==============================================

To check the state of an Amazon Sagemaker training job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTrainingSensor`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sagemaker_training]
    :end-before: [END howto_sensor_sagemaker_training]

.. _howto/sensor:SageMakerTransformSensor:

Wait on an Amazon SageMaker transform job state
===============================================

To check the state of an Amazon Sagemaker transform job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sagemaker_transform]
    :end-before: [END howto_sensor_sagemaker_transform]

.. _howto/sensor:SageMakerTuningSensor:

Wait on an Amazon SageMaker tuning job state
============================================

To check the state of an Amazon Sagemaker hyperparameter tuning job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTuningSensor`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sagemaker_tuning]
    :end-before: [END howto_sensor_sagemaker_tuning]

.. _howto/sensor:SageMakerEndpointSensor:

Wait on an Amazon SageMaker endpoint state
==========================================

To check the state of an Amazon Sagemaker endpoint until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerEndpointSensor`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sagemaker_endpoint]
    :end-before: [END howto_sensor_sagemaker_endpoint]

.. _howto/sensor:SageMakerPipelineSensor:

Wait on an Amazon SageMaker pipeline execution state
====================================================

To check the state of an Amazon Sagemaker pipeline execution until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerPipelineSensor`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_pipeline.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sagemaker_pipeline]
    :end-before: [END howto_sensor_sagemaker_pipeline]

.. _howto/sensor:SageMakerAutoMLSensor:

Wait on an Amazon SageMaker AutoML experiment state
===================================================

To check the state of an Amazon Sagemaker AutoML job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerAutoMLSensor`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_auto_ml]
    :end-before: [END howto_operator_sagemaker_auto_ml]

.. _howto/sensor:SageMakerProcessingSensor:

Wait on an Amazon SageMaker processing job state
================================================

To check the state of an Amazon Sagemaker processing job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerProcessingSensor`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sagemaker_processing]
    :end-before: [END howto_sensor_sagemaker_processing]

Reference
---------

* `AWS boto3 library documentation for Sagemaker <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html>`__
* `Amazon SageMaker Developer Guide <https://docs.aws.amazon.com/sagemaker/latest/dg/whatis.html>`__
