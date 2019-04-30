.. _azure_configuration_options:

Configuration Options
---------------------

Kafka
^^^^^

``kafka.topic``
  The name of the kafka topic to write data from Kafka into Azure Blob.

  * Type: String
  * Default: No Default
  * Importance: high
  * Required: yes

Connector
^^^^^^^^^

``name``
  Name of the Azure Blob Connector

  * Type: String
  * Default: No Default
  * Importance: high
  * Required: yes

``connector.class``
  Class of the connector

  * Type: class
  * Default: No Default
  * Valid Values: [io.confluent.connect.azblob.storage.AzBlobStorage]
  * Importance: high
  * Required: yes

``task.max``
  Number of Tasks

  * Type: int
  * Default: 1
  * Valid Values: [1,...]
  * Importance: high
  * Required: no

``format.class``
  The format class to use when writing data to the store.

  * Type: class
  * Default: No Default
  * Valid Values: [io.confluent.connect.azblob.format.json.JsonFormat,
                   io.confluent.connect.azblob.format.avro.AvroFormat,
                   io.confluent.connect.azblob.format.bytearray.ByteArrayFormat]
  * Importance: high
  * Required: yes

``flush.size``
  Number of records written to store before invoking file commits.

  * Type: int
  * Defaul: No Default
  * Valid Values: [1,...]
  * Importance: high
  * Required: yes

``rotate.interval.ms``
  The time interval in milliseconds to invoke file commits. This configuration ensures that file commits are invoked every configured interval. This configuration is useful when data ingestion rate is low and the connector didn't write enough messages to commit files. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: high
  * Required: no

``rotate.schedule.interval.ms``
  The time interval in milliseconds to periodically invoke file commits. This configuration ensures that file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. Commit will be performed at scheduled time regardless previous commit time or number of messages. This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: medium
  * Required: no

``schema.cache.size``
  The size of the schema cache used in the Avro converter.

  * Type: int
  * Default: 1000
  * Importance: low
  * Required: no

``schema.compatibility``
  Supported Schema Types

  * Type: String
  * Default: NONE
  * Valid Values: [NONE, BACKWARD, FORWARD, FULL]
  * Importance: high
  * Required: no

``retry.backoff.ms``
  The retry backoff in milliseconds. This config is used to notify Kafka connect to retry delivering a message batch or performing recovery in case of transient exceptions.

  * Type: long
  * Default: 5000
  * Importance: low
  * Required: no

``filename.offset.zero.pad.width``
  Width to zero pad offsets in store's filenames if offsets are too short in order to provide fixed width filenames that can be ordered by simple lexicographic sorting.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: low
  * Required: no

Azure
^^^^^

``azblob.storageaccount.connectionstring``
  The connection string for the Azure Storage Account.

  * Type: string
  * Default: No Default
  * Importance: high
  * Required: yes

``azblob.containername``
  The name of the container within the Azure Storage Account where the files are to written.

  * Type: string
  * Default: No Default
  * Importance: high
  * Required: yes

``avro.codec``
  The Avro compression codec to be used for output files. Available values: null, deflate, snappy and bzip2 (codec source is org.apache.avro.file.CodecFactory)

  * Type: string
  * Default: null
  * Importance: low
  * Required: no

Storage
^^^^^^^

``storage.class``
  The underlying storage layer.

  * Type: class
  * Default: No Default
  * Importance: high
  * Required: yes

``topics.dir``
  Top level directory to store the data ingested from Kafka.

  * Type: string
  * Default: topics
  * Importance: high
  * Required: no

``store.url``
  Store's connection URL, if applicable.

  * Type: string
  * Default: null
  * Importance: high
  * Required: no

``directory.delim``
  Directory delimiter pattern

  * Type: string
  * Default: /
  * Importance: medium
  * Required: no

``file.delim``
  File delimiter pattern

  * Type: string
  * Default: +
  * Importance: medium
  * Required: no

Partitioner
^^^^^^^^^^^

``partitioner.class``
  The partitioner to use when writing data to the store. You can use ``DefaultPartitioner``, which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to different directories according to the value of the partitioning field specified in ``partition.field.name``; ``TimeBasedPartitioner``, which partitions data according to ingestion time.

  * Type: class
  * Default: io.confluent.connect.storage.partitioner.DefaultPartitioner
  * Valid Values: [DefaultPartitioner, FieldPartitioner, TimeBasedPartitioner]
  * Importance: high
  * Required: no
  * Dependents: ``partition.field.name``, ``partition.duration.ms``, ``path.format``, ``locale``, ``timezone``, ``schema.generator.class``

``schema.generator.class``
  The schema generator to use with partitioners.

  * Type: class
  * Default: io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator
  * Importance: high
  * Required: no

``partition.field.name``
  The name of the partitioning field when FieldPartitioner is used.

  * Type: string
  * Default: ""
  * Importance: medium
  * Required: no

``partition.duration.ms``
  The duration of a partition milliseconds used by ``TimeBasedPartitioner``. The default value -1 means that we are not using ``TimeBasedPartitioner``.

  * Type: long
  * Default: -1
  * Importance: medium
  * Required: no

``path.format``
  This configuration is used to set the format of the data directories when partitioning with ``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp to proper directories strings. For example, if you set ``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH``, the data directories will have the format ``/year=2015/month=12/day=07/hour=15/``.

  * Type: string
  * Default: ""
  * Importance: medium
  * Required: no

``locale``
  The locale to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium
  * Required: no

``timezone``
  The timezone to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium
  * Required: no

``timestamp.extractor``
  The extractor that gets the timestamp for records when partitioning with ``TimeBasedPartitioner``. It can be set to ``Wallclock``, ``Record`` or ``RecordField`` in order to use one of the built-in timestamp extractors or be given the fully-qualified class name of a user-defined class that extends the ``TimestampExtractor`` interface.

  * Type: string
  * Default: Wallclock
  * Importance: medium
  * Required: no

``timestamp.field``
  The record field to be used as timestamp by the timestamp extractor.

  * Type: string
  * Default: timestamp
  * Importance: medium
  * Required: no

