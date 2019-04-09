# Kafka Connect Azure

The following guide provides step by step instructions how to build and integrate Azure Blob Connector with Kafka platform. It can be integrated as a part of Kafka stack or Confluent platform.

The Azure Blob connector, currently available as a sink, allows you to export data from Kafka topics to Azure Blob in either Avro or JSON formats.

Being a sink, the Azure Blob connector periodically polls data from Kafka and in turn uploads it to Azure Blob Storage. A partitioner is used to split the data of every Kafka partition into chunks. Each chunk of data is represented as an Azure Blob, whose key name encodes the topic, the Kafka partition and the start offset of this data chunk. If no partitioner is specified in the configuration, the default partitioner which preserves Kafka partitioning is used. The size of each data chunk is determined by the number of records written to Azure Blob and by schema compatibility.

## Resources
- [Connector GitHub repository](https://github.com/akshayhashedIn/kafka-connect-storage-cloud/tree/feature/Azure_Blob_Storage_Container)
- [Kafka Developer Manual]()
- [Confluent Kafka Connect](https://docs.confluent.io/current/connect/index.html)

## Installation procedure
The connector provided in this project assumes launching will be done on a server capable of running an azure Kafka connector in a standalone mode or in a distributed mode.

### Version Support
The connector's code is written in Java 8 and is compatible in Java 10 environment.
Azure Blob connector requires
|Platform|Versions|
|--------|--------|
| Java   | Above 8 |
| Gradle  |5 =<    |
| Confluent Platform | 5.2.1 =< |
| Docker  | 18.09=< |
| Kafka  |         |

### Installation of Azure Blob Connector on Confluent Platform
Clone and build the following repositories as instructed

Clone and build Kafka

```bash
git clone  https://github.com/apache/kafka.git
cd kafka
gradle
./gradlew installAll
```

Clone and compile kafka-connect-azure
```bash
git clone https://github.com/akshayhashedIn/kafka-connect-storage-cloud.git
cd kafka-connect-storage-cloud
mvn clean compile package
```

Download the new confluent package which is available  [here](https://www.confluent.io/download/)

Use a docker(docker-compose.yml file) for running Kafka-connect-azure which is available here.

Edit the path inside the docker-compose.yml file under **kafka-connect** **volumes** layer with the path to confluent directory in your system.

The JAR files which are produced by the ```compile package``` command which was executed in the above instructions. The files can be found in kafka-connect-storage-cloud/kafka-connect-azure/target/ directory.

To install the connector at the target server location for Confluent platform, check the project target folder(/kafka-connect-storage-cloud/kafka-connect-azure/target) it should contain the artifact folder ``` kafka-connect-azure-<ver>-SNAPSHOT-package ``` where ```<ver>``` indicates your version of confluent Follow the same directory structure you find in the build artifact and copy files into ``` CONFLUENT_HOME ``` directories:

execute these commands inside the *kafka-connect-azure* directory
```bash

cp -r target/kafka-connect-sqs-<ver>-SNAPSHOT-package/share/java/* /CONFLUENT-HOME/share/java/

cp -r target/kafka-connect-azure-<ver>-SNAPSHOT-package/etc/* /CONFLUENT-HOME/etc/

cp -r target/kafka-connect-azure-<ver>-SNAPSHOT-package/share/doc/* /CONFLUENT-HOME/share/doc/

```

## Kafka Connect Azure Plugin Deployment considerations

Before you begin, create an azure storage account, generate the key for your account and create a container which will poll the data from kafka.

Kafka connect can run in two ways Standalone and Distributed mode.

In standalone mode, a single process runs all the connectors. It is not fault tolerant. Since it uses only a single process, it is not scalable. Standalone mode is used for proof of concept and demo purposes, integration or unit testing, and it is managed through CLI.

In distributed mode, multiple workers run Kafka Connect and are aware of each others existence, which can provide fault tolerance and coordination between them and during the event of reconfiguration. In this mode, Kafka Connect is scalable and fault tolerant, so it is generally used in production deployment. Distributed mode provides flexibility, scalability and high availability, it's mostly used in production in cases of heavy data volume, and it is managed through REST interface.

### Standalone mode
Use the following command when running in standalone mode
```bash
CONFLUENT_HOME>bin/connect-standalone etc/kafka/connect-standalone.properties etc/kafka-connect-azure/quickstart-azblob.properties
```

Before starting the connector, please make sure that the configurations in `etc/kafka-connect-azure/quickstart-azblob.properties` are properly set to your configurations of azure, for example
- In the `AccountName` field add the name of your azure storage account.
- In the `AccountKey` field add the key generated.
- In the `azblob.containername` field add the name of your Azure Blob Storage Container.

Here ```connect-standalone.properties``` is used to configure worker tasks and ```quickstart-azblob.properties``` is used to configure the connector itself.

### Distributed mode
Another way of getting started is using the docker file

Run the docker file by running the compose command. Be sure to be in the same directory as that of the docker file and also be sure to have super user previlages before running the command.
```bash
sudo docker-compose up
```
The above command will set up a User interface which can be accessed on ``` localhost:9021 ```
The connector can be created using the user interface on localhost:9021. A connector can also be created by using REST calls.
Kafka Connector configuration sent in REST calls has the same config properties
```json

{
    "name": "azure-blob",
    "config": {
        "connector.class": "io.confluent.connect.azblob.AzBlobSinkConnector",
        "tasks.max": 1,
        "topics": "connect-test",
        "flush.size": 1,
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "format.class": "io.confluent.connect.azblob.format.avro.AvroFormat",
        "storage.class": "io.confluent.connect.azblob.storage.AzBlobStorage",
        "schema.compatibility": "NONE",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
"azblob.storageaccount.connectionstring":"DefaultEndpointsProtocol=https;AccountName=<Your-Account-Name>;AccountKey=<Your-Account-Key>;EndpointSuffix=core.windows.net",
        "azblob.containername": "<Your-Container-Name>",
        "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator"
    }
}
```
### GET /connectors
Check the available connectors. Assuming that you are running on the recommended docker.
```bash
curl -X GET -H "Accept: application/json" http://localhost:9021/2.0/management/connect/connectors/
```

## Streaming Data from Kafka into Kinetica

.. _azure_configuration_options:

Configuration Options
---------------------

Connector
^^^^^^^^^

``format.class``
  The format class to use when writing data to the store.

  * Type: class
  * Importance: high

``flush.size``
  Number of records written to store before invoking file commits.

  * Type: int
  * Importance: high

``rotate.interval.ms``
  The time interval in milliseconds to invoke file commits. This configuration ensures that file commits are invoked every configured interval. This configuration is useful when data ingestion rate is low and the connector didn't write enough messages to commit files. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: high

``rotate.schedule.interval.ms``
  The time interval in milliseconds to periodically invoke file commits. This configuration ensures that file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. Commit will be performed at scheduled time regardless previous commit time or number of messages. This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: medium

``schema.cache.size``
  The size of the schema cache used in the Avro converter.

  * Type: int
  * Default: 1000
  * Importance: low

``retry.backoff.ms``
  The retry backoff in milliseconds. This config is used to notify Kafka connect to retry delivering a message batch or performing recovery in case of transient exceptions.

  * Type: long
  * Default: 5000
  * Importance: low

``filename.offset.zero.pad.width``
  Width to zero pad offsets in store's filenames if offsets are too short in order to provide fixed width filenames that can be ordered by simple lexicographic sorting.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: low

Azure
^^

``azblob.storageaccount.connectionstring``
  The connection string for the Azure Storage Account.

  * Type: string
  * Importance: high

``azblob.containername``
  The name of the container within the Azure Storage Account where the files are to written.

  * Type: string
  * Importance: high


``avro.codec``
  The Avro compression codec to be used for output files. Available values: null, deflate, snappy and bzip2 (codec source is org.apache.avro.file.CodecFactory)

  * Type: string
  * Default: null
  * Importance: low

Storage
^^^^^^^

``storage.class``
  The underlying storage layer.

  * Type: class
  * Importance: high

``topics.dir``
  Top level directory to store the data ingested from Kafka.

  * Type: string
  * Default: topics
  * Importance: high

``store.url``
  Store's connection URL, if applicable.

  * Type: string
  * Default: null
  * Importance: high

``directory.delim``
  Directory delimiter pattern

  * Type: string
  * Default: /
  * Importance: medium

``file.delim``
  File delimiter pattern

  * Type: string
  * Default: +
  * Importance: medium

Partitioner
^^^^^^^^^^^

``partitioner.class``
  The partitioner to use when writing data to the store. You can use ``DefaultPartitioner``, which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to different directories according to the value of the partitioning field specified in ``partition.field.name``; ``TimeBasedPartitioner``, which partitions data according to ingestion time.

  * Type: class
  * Default: io.confluent.connect.storage.partitioner.DefaultPartitioner
  * Importance: high
  * Dependents: ``partition.field.name``, ``partition.duration.ms``, ``path.format``, ``locale``, ``timezone``, ``schema.generator.class``

``schema.generator.class``
  The schema generator to use with partitioners.

  * Type: class
  * Importance: high

``partition.field.name``
  The name of the partitioning field when FieldPartitioner is used.

  * Type: string
  * Default: ""
  * Importance: medium

``partition.duration.ms``
  The duration of a partition milliseconds used by ``TimeBasedPartitioner``. The default value -1 means that we are not using ``TimeBasedPartitioner``.

  * Type: long
  * Default: -1
  * Importance: medium

``path.format``
  This configuration is used to set the format of the data directories when partitioning with ``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp to proper directories strings. For example, if you set ``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH``, the data directories will have the format ``/year=2015/month=12/day=07/hour=15/``.

  * Type: string
  * Default: ""
  * Importance: medium

``locale``
  The locale to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``timezone``
  The timezone to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``timestamp.extractor``
  The extractor that gets the timestamp for records when partitioning with ``TimeBasedPartitioner``. It can be set to ``Wallclock``, ``Record`` or ``RecordField`` in order to use one of the built-in timestamp extractors or be given the fully-qualified class name of a user-defined class that extends the ``TimestampExtractor`` interface.

  * Type: string
  * Default: Wallclock
  * Importance: medium

``timestamp.field``
  The record field to be used as timestamp by the timestamp extractor.

  * Type: string
  * Default: timestamp
  * Importance: medium

