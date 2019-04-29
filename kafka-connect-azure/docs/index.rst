Kafka Connect Azure Connector
=============================

The Azure Blob connector, currently available as a sink, allows you to export data from Kafka topics to Azure Blob in either Avro or JSON formats.

Being a sink, the Azure Blob connector periodically polls data from Kafka and in turn uploads it to Azure Blob Storage. A partitioner is used to split the data of every Kafka partition into chunks. Each chunk of data is represented as an Azure Blob, whose key name encodes the topic, the Kafka partition and the start offset of this data chunk. If no partitioner is specified in the configuration, the default partitioner which preserves Kafka partitioning is used. The size of each data chunk is determined by the number of records written to Azure Blob and by schema compatibility.

The following guide provides step by step instructions how to build and integrate Azure Blob Connector with Kafka platform or Confluent platform.

The connector class which is available in Azure Blob is
``io.confluent.connect.azblob.AzBlobSinkConnector`` a Sink connector which
streams data from Kafka into Azure Blobs.

Features
--------

- **Exactly Once Delivery** : The connector makes sure that the messages are delivered from Kafka into Azure Blob Storage exactly once.

- **Pluggable Data Format with or without Schema** : Out of the box, the connector supports writing data to Azure Blob in Avro and JSON format. Besides records with schema, the connector supports exporting plain JSON records without schema in text files, one record per-line.In general, the connector may accept any format that provides an implementation of the Format interface.

- **Schema Evolution** : When schemas are used, the connector supports schema evolution based on schema compatibility modes. The available modes are: NONE, BACKWARD, FORWARD and FULL and a selection can be made by setting the property schema.compatibility in the connector's configuration.When the connector observes a schema change, it decides whether to roll the file or project the record to the proper schema according to the schema.compatibility configuration in use.

- **Pluggable Partitioner** : The connector comes out of the box with partitioners that support default partitioning based on Kafka partitions, field partitioning, and time-based partitioning in days or hours. You may implement your own partitioners by extending the Partitioner class.Additionally, you can customize time based partitioning by extending the TimeBasedPartitioner class.

Install Connector Using Confluent Hub
-------------------------------------

Confluent Hub Client must be installed. This is installed by default with Confluent Platform commercial features.
Navigate to your Confluent Platform installation directory and run this command to install the latest (latest) connector version. The connector must be installed on every machine where Connect will be run.

``confluent-hub install confluentinc/kafka-connect-azure:latest``

If you do not have Confluent Platform installed and running, you can install the connector using the Confluent Hub client (recommended) or manually download the ZIP file.

Install Connector Manually using Connector Plugin
-------------------------------------------------
A Kafka Connect plugin is simply a set of JAR files where Kafka Connect can find an implementation of one or more connectors, transforms, and/or converters. Kafka Connect isolates each plugin from one another so that libraries in one plugin are not affected by the libraries in any other plugins.

A Kafka Connect plugin is either:
 1. an **uber JAR** containing all of the classfiles for the plugin and its third-party dependencies in a single JAR file.

 ::

   kafka-connect-storage-cloud/kafka-connect-azure/target/kafka-<ver>-connector-azure-<ver>-jar-with-dependencies.jar

 2. a directory on the file system that contains the JAR files for the plugin and its third-party dependencies.

 ``kafka-connect-storage-cloud/kafka-connect-azure/target/kafka-<ver>-connector-azure-<ver>-package``

 ``|-- etc``
    ``|-- kafka-connect-azure``
        ``|-- quickstart--azblob.properties``
 ``|-- share``
    ``|-- doc``
        ``|-- kafka-connect-azure``
    ``|-- java``
      ``|-- kafka-connect-azure``

**Installing the connector on plain Kafka stack**

To install the connector at the target server location for plain Kafka, copy the uber JAR kafka-connector-azure-jar-with-dependencies.jar into KAFKA_HOME/libs/ folder and make sure configuration in KAFKA_HOME/config/connect-distributed.properties and KAFKA_HOME/config/connect-standalone.properties files matches the configuration you've tested with your Kafka connector. Any additional properties files you might need should go in the same folder KAFKA_HOME/config/


**Installing the connector on Confluent Platform**


To install the connector at the target server location for Confluent platform, check the project target folder it should contain the artifact folder kafka-connector-azure-package Follow the same directory structure you find in the build artifact and copy files into CONFLUENT_HOME directories:

 ::

   mkdir /CONFLUENT_HOME/share/java/kafka-connect-azure
   cp target/kafka-<ver>-connector-azure-<ver>-package/share/java/*
   /CONFLUENT_HOME/share/java/kafka-connect-azure/

   mkdir /CONFLUENT_HOME/etc/kafka-connect-azure
   cp target/kafka-<ver>-connector-azure-<ver>-package/etc/*
   /CONFLUENT_HOME/etc/kafka-connect-azure/

   mkdir /CONFLUENT_HOME/share/doc/kafka-connect-azure
   cp target/kafka-<ver>-connector-azure-<ver>-package/share/doc/*
   /CONFLUENT_HOME/share/doc/kafka-connect-azure/


All Configuration Variables
----------------------------------

The Kafka Connect Azure is configured through AzBlobSinkConnectorConfig
class using ``quickstart-azblob.properties`` file that accepts the
following parameters:

+----------------------------------------+------------+---------------------------------------------------------------------------+
| Property Name                          | Required   | Description                                                               |
+========================================+============+===========================================================================+
| name                                   | YES        | Name of the Azure Blob Connector                                          |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| connector.class                        | YES        | Should be io.confluent.connect.azblob.AzBlobSinkConnector                 |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| tasks.max                              | YES        | Number of Tasks                                                           |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| azblob.storageaccount.connectionstring | YES        | Connection String Of Azure Blob Storage                                   |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| azblob.containername                   | YES        | Name Of Azure Blob Container                                              |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| kafka.topic                            | YES        | Name of the Kafka Topic                                                   |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| storage.class                          | NO         | Should be io.confluent.connect.azblob.storage.AzBlobStorage               |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| format.class                           | YES        | Format Type can be                                                        |
|                                        |            | io.confluent.connect.azblob.format.json.JsonFormat                        |
|                                        |            | io.confluent.connect.azblob.format.avro.AvroFormat                        |
|                                        |            | io.confluent.connect.azblob.format.bytearray.ByteArrayFormat              |
|                                        |            | io.confluent.connect.azblob.format.parquet.ParquetFormat                  |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| schema.generator.class                 | NO         | Should be io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| partitioner.class                      | NO         | Should be io.confluent.connect.storage.partitioner.DefaultPartitioner     |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| schema.compatibility                   | YES        | Schema Compatible Types can be NONE, BACKWARD, FORWARD, FULL              |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| partition.field.name                   | NO         | Name of the Partition Field                                               |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| partition.duration.ms                  | NO         | Duration of Partition                                                     |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| path.format                            | NO         | Format of path                                                            |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| locale                                 | NO         | Locale                                                                    |
+----------------------------------------+------------+---------------------------------------------------------------------------+
| timezone                               | NO         | Time Zone                                                                 |
+----------------------------------------+------------+---------------------------------------------------------------------------+

Edit the configuration file quickstart-azblob.properties for the
connector:

::

    name = <NameOfTheSinkConnector>
    connector.class = io.confluent.connect.azblob.AzBlobSinkConnector
    tasks.max = 1
    kafka.topic = <NameOfTheKafkaTopic>
    azblob.storageaccount.connectionstring = <Connection-String>
    azblob.containername = <Name-Of-Azure-Blob-Container>
    format.class = <Format-Of-Data>
    schema.compatibility = NONE

Resources
---------

-  `Confluent
   Documentation <https://docs.confluent.io/current/index.html>`__
-  `Kafka developers
   Guide <https://kafka.apache.org/10/documentation/streams/developer-guide/>`__
-  `Azure Blob Connector GitHub
   Repo <https://github.com/hashedin/kafka-connect-azure>`__

Prerequisites
-------------

1. You must have an azure storage account.
2. Create a container in azure storage account.
3. Create a Kafka topic called ``azure-quickstart``.

Quick Start
-----------

Kafka connect can run in two ways Standalone and Distributed mode.

In standalone mode, a single process runs all the connectors. It is not
fault tolerant. Since it uses only a single process, it is not scalable.
Standalone mode is used for proof of concept and demo purposes,
integration or unit testing, and it is managed through CLI.

In distributed mode, multiple workers run Kafka Connect and are aware of
each others' existence, which can provide fault tolerance and
coordination between them and during the event of reconfiguration. In
this mode, Kafka Connect is scalable and fault tolerant, so it is
generally used in production deployment. Distributed mode provides
flexibility, scalability and high availability, it's mostly used in
production in cases of heavy data volume, and it is managed through REST
interface.

Step 1: Start Confluent Services using one Command
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the following command when running in standalone mode

::

    CONFLUENT_HOME> ./bin/confluent start

Every service will start in order, printing a message with its status:

::

    Starting zookeeper
    zookeeper is [UP]
    Starting kafka
    kafka is [UP]
    Starting schema-registry
    schema-registry is [UP]
    Starting kafka-rest
    kafka-rest is [UP]
    Starting connect
    connect is [UP]
    Starting ksql-server
    ksql-server is [UP]
    Starting control-center
    control-center is [UP]

Confluent control center will be available in localhost:9021 The
connectors an be setup by the GUI

To stop the server run

::

    CONFLUENT_HOME> ./bin/confluent stop

Step 2: Add Records
~~~~~~~~~~~~~~~~~~~

To import a few records with a simple schema in Kafka, start the Avro console producer as follows:

::

      ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic azure-quickstart \
    --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

Then, in the console producer, type in:

::

    {"f1": "value1"}
    {"f1": "value2"}
    {"f1": "value3"}
    {"f1": "value4"}
    {"f1": "value5"}
    {"f1": "value6"}
    {"f1": "value7"}
    {"f1": "value8"}
    {"f1": "value9"}

The nine records entered are published to the Kafka topic ``azure-quickstart`` in Avro format.

Step 3: Start the Azure Blob Connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before starting the connector, please make sure that the configurations in ``etc/kafka-connect-azure/quickstart-azblob.properties`` are properly set to your configurations of azure. At a minimum, you need to set ``azblob.storageaccount.connectionstring`` and ``azblob.containername``.

Standalone Mode
^^^^^^^^^^^^^^^

Use the following command when running in standalone mode

::

    CONFLUENT_HOME>bin/connect-standalone etc/kafka/connect-standalone.properties etc/kafka-connect-azure/quickstart-azblob.properties

Distributed Mode
^^^^^^^^^^^^^^^^

Another way of getting started is using the docker file.

Run the docker file by running the compose command. Be sure to be in the
same directory as that of the docker file and also be sure to have super
user privileges before running the command.

::

    sudo docker-compose up

The above command will set up a User interface which can be accessed on
localhost:9021 The connector can be created using the user interface on
localhost:9021. A connector can also be created by using REST calls.
Kafka Connector configuration sent in REST calls has the same config
properties

::

    {
    "name": "AzureBlobSinkConnector",
    "config": {
        "connector.class": "io.confluent.connect.azblob.AzBlobSinkConnector",
        "tasks.max": "1",
        "topics": "azure-quickstart",
        "flush.size": "1",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
        "format.class": "io.confluent.connect.azblob.format.json.JsonFormat",
        "storage.class": "io.confluent.connect.azblob.storage.AzBlobStorage",
        "schema.compatibility": "NONE",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        "azblob.storageaccount.connectionstring": "<Your-Connection-String>",
        "azblob.containername": "<Azure-Blob-Container-Name>",
        "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
    }
  }

POST /connectors
~~~~~~~~~~~~~~~~

Create a new connector (connector object is returned):

::

    curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" --data ‘{
    "name": "AzureBlobSinkConnector",
    "config": {
        "connector.class": "io.confluent.connect.azblob.AzBlobSinkConnector",
        "tasks.max": "1",
        "topics": "azure-quickstart",
        "flush.size": "1",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
        "format.class": "io.confluent.connect.azblob.format.json.JsonFormat",
        "storage.class": "io.confluent.connect.azblob.storage.AzBlobStorage",
        "schema.compatibility": "NONE",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        "azblob.storageaccount.connectionstring": "<Your-Connection-String>",
        "azblob.containername": "<Azure-Blob-Container-Name>",
        "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
    }
    }’ http://localhost:8083/connectors

Next, start the Azure Blob Sink connector by loading its configuration with
the following command:

``confluent load azure-sink``

To check that the connector started successfully, view the connect
worker's log by running:

``confluent log connect``

Azure Blob Connector Credentials
----------------------------------

The ``AZ_STORAGEACCOUNT_CONNECTIONSTRING`` and ``AZ_CONTAINER_NAME`` environment variables accessible to the Connect worker processes where the connector will be deployed:

    ``export AZ_STORAGEACCOUNT_CONNECTIONSTRING=<Your_StorageAccount_ConnectionString>``
    ``export AZ_CONTAINER_NAME=<Your_Container_Name>``

Unit Testing
----------------
To run all the testcases, write the following command in the terminal,
    ``mvn test``

System Testing
-----------------
This test will demonstrate the Azure-Kafka-connector sink in standalone mode. The standalone mode should be used only for testing. You should use distributed mode for a production deployment.

Create configuration file in ``quickstart-azblob.properties`` based on example below.

::

  name=kafka-connect-azure
  connector.class=io.confluent.connect.azblob.AzBlobSinkConnector
  tasks.max=1
  kafka.topic='azure-quickstart'
  azblob.storageaccount.connectionstring='DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;'
  azblob.containername='AzureBlobContainer'

The rest of this system test will require three terminal windows.

1. In terminal 1, start zookeeper and kafka:

 ::

   $ cd <path/to/Kafka>
   $ bin/zookeeper-server-start.sh config/zookeeper.properties &
   $ bin/kafka-server-start.sh config/server.properties

2. In terminal 2, start kafka sink connectors:

 ::

   $ connect-standalone connect-standalone.properties \
    quickstart-azblob.properties

3. Verify that data is copied to topic, mentioned in the ``quickstart-azblob.properties``


For more information,please refer to:

Azure Blob Storage Documentation : https://docs.microsoft.com/en-us/azure/storage/

Kafka-Connect Documentation : https://docs.confluent.io/current/connect/index.html