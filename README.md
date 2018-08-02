mongo_bigquery_archiver is a Python 2.7 Mongo to Bigquery connector that automatically backs up Mongo records to Bigquery tables via the streaming API. As Bigquery is write-only and requires a schema and Mongo is schemaless and supports the full range of CRUD operations, this makes some sacrifices in the transition.

What it does:

1) Creates a view in Bigquery on top of the table that reflects the current state of Mongo, with the table representing all historical states.

2) Upon creation of a new document in Mongo, creates a document in BigQuery based upon the user-provided schema.

3) Upon document update, creates a new document in BigQuery based upon the user-provided schema with an updated timestamp.

4) Upon deletion of a document, remove that document from the generated view in BigQuery.

5) Given a primary key to filter on, creates a view that shows the latest document for every given primary key.

6) Automatically determine the schema for a given Mongo collection. 


### Usage

#### I.
We recommend using Kubernetes to deploy the mongo_bigquery_archiver. deployment.prod.yaml is an example configuration for a Kubernetes deployment, and configmap.prod.yaml is an example deployment of the configuration file for the bigquery_archiver to use. 

Alternatively, you can modify the Dockerfile to load a config.json file like the one specified in configmap.prod.yaml. The configuration JSON file should be copied to /config-local/config.json in this case.

Alternatively, you can run the script manually, by running `pip install -r /tmp/requirements.txt` to install the necessary requirements and then running the script with `python bigquery_archiver.py`, assuming the config.json file is in /config-local/config.json. 

The configuration file is of the following format:

```{
    "MONGO_CLIENT": The Mongo connection URL to connect to your server or replica set. Example: "mongodb://localhost:27017/?replicaset=rs0",
    "SERVICE_ACCOUNT_FILE": The path to your service account file. If deploying to Kubernetes, we recommend mounting it as a secret and putting the path to the file here.
    "BIGQUERY_PROJECT": Your Bigquery project.
    "BIGQUERY_DATASET": The Bigquery dataset to generate tables under.
}```


#### II.
mongo_bigquery_archiver, once deployed, reads further configuration settings from your Mongo server. It checks the `config` database for the collection `bigquery_archiver_settings`. If they don't exist, it will create both and then wait for them to be updated. To begin archiving Mongo collections, you need to specify a mapping in `bigquery_archiver_settings`. Each document corresponds to one Mongo collection and one generated Bigquery table. The format of each document is the following:

`mongo_database`:   The name of the database containing the collection to archive

`mongo_collection`:   The name of the collection to archive

`bigquery_table`:   The name of the Bigquery table to create to store this data. The Bigquery dataset will always be acquisitions.

`columns`:   Optional. An array of Column specifications, defining the Mongo properties to store as Bigquery columns. If you do not specify one, one will automatically be generated upon first load.

`backfill_full_collection`:   If True, will take all historic data from the Mongo collection and load into Bigquery upon detecting the first change. The backfill will only happen once.

`dryrun`:   If True, will autogenerate schema and log changes, but not perform any Bigquery operations.

Column specifications are of the following format:

`name`:    Column name in Bigquery

`type`:    Bigquery type. Possible values are STRING, BYTES, INTEGER, FLOAT, BOOLEAN, TIMESTAMP, DATE, TIME, DATETIME, and RECORD.

`mongo_field`:   Property in Mongo to store in this column in Bigquery. Supports nested fields, `such as subdocument.address`. If _id, the ObjectID will be cast to a string and used instead.

`fields`:   An array of Column specifications, only used in the case of the RECORD type to define the schema of the RECORD. Fields specified within here will query on the corresponding subdocument specified by the name field.

`primary_key`:   If set to True, will be used as a primary key for Bigquery. A view will be created with the name bigquery_table+'_latest_view' that groups by the primary_key and will only show the most recent entry for each primary_key value.

`mode`: Defines the mode of the column, defaulting to NULLABLE. Can also be REQUIRED or REPEATED to enable array values.

An example configuration document:
```
{
    "mongo_database" : "bidding",
    "mongo_collection" : "ad_cpta",
    "bigquery_table" : "facebook_ad_cpta",
    "backfill_full_collection" : true,
    "columns" : [
        {
            "name" : "tracking_code",
            "type" : "INTEGER",
            "mongo_field" : "tracking_code"
        },
        {
            "name" : "midnight_to_timestamp",
            "type" : "RECORD",
            "mongo_field" : "midnight_to_timestamp",
            "fields" : [
                {
                    "name" : "cpta",
                    "type" : "FLOAT"
                },
                {
                    "name" : "activations",
                    "type" : "INTEGER"
                },
                {
                    "name" : "spend",
                    "type" : "FLOAT"
                },
                {
                    "name" : "date_start",
                    "type" : "STRING"
                },
                {
                    "name" : "ad_spend_id",
                    "type" : "STRING"
                }
            ]
        },
        {
            "name" : "created_on_timestamp",
            "type" : "FLOAT",
            "mongo_field" : "created_on_timestamp"
        },
        {
            "name" : "lifetime_to_timestamp",
            "type" : "RECORD",
            "mongo_field" : "lifetime_to_timestamp",
            "fields" : [
                {
                    "name" : "cpta",
                    "type" : "FLOAT"
                },
                {
                    "name" : "activations",
                    "type" : "INTEGER"
                },
                {
                    "name" : "spend",
                    "type" : "FLOAT"
                }
            ]
        }
    ]
}
```

The Bigquery archiver will automatically refresh. If backfill_full_collection is true, it'll load the entire collection into Bigquery, otherwise it'll only pick up new changes. If an error occurs, it'll be added to the lastFailure row in the selfsame configuration document. If it does not, check the logs of the mongo_bigquery_archiver deployment in Kubernetes.