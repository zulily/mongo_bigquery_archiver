"""
Copyright 2018 zulily, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

mongo_bigquery_archiver is a service that, when properly configured, automatically takes
specified collections in a Mongo server and creates a living view of their latest state
and a historical log of operations on the collection in a Bigquery table.

"""

import multiprocessing
import traceback
import logging
import time
import json
import datetime
import bson
from pymongo import MongoClient
from google.cloud import bigquery

try:
    CONFIG_FILE = open('/config-local/config.json')
    CONFIG = json.loads(CONFIG_FILE.read())
    CONFIG_FILE.close()
except IOError:
    logging.error('Configuration not able to be read at /config-local/config.json, exiting.')
logging.basicConfig(level=logging.DEBUG)

# The collection that contains the collections to monitor has records in the following format:
# mongo_database: the mongo database where the collection is
# mongo_collection: the name of the collection to backup to BigQuery
# bigquery_table: the table to stream the mongo collection to
# columns: an array containing documents of the following format
# - name: the name of the column in BigQuery
# - type: the type of the column in BigQuery, from the following list:
# -- STRING, BYTES, INTEGER, FLOAT, BOOLEAN, TIMESTAMP, DATE, TIME, DATETIME, RECORD
# - mongo_field: the field in mongo that should be read for this column
# - primary_key: if present and true, is a primary key we can use for updates

def build_schema_field(column):
    """ Given a column definition, return the bigquery SchemaField object"""
    mode = column.get('mode', 'NULLABLE')
    if column['type'] != "RECORD":
        return bigquery.schema.SchemaField(column['name'], column['type'], mode)
    fields = set([build_schema_field(field) for field in column['fields']])
    return bigquery.schema.SchemaField(column['name'], column['type'], mode, fields=fields)

def get_row(document, columns):
    """ Given a document and a set of column definitions, return the dictionary representing the row to upload to bigquery
        that contains only the fields matching the column definitions"""
    row_to_upload = {}
    for column in columns:
        try:
            mongo_field = column.get('mongo_field')
            if not mongo_field:
                mongo_field = column.get('name')
            if column['type'] != "RECORD":
                if mongo_field == '_id':
                    # Mongo default document ObjectID, treat it specially
                    row_to_upload[column['name']] = str(document['_id'])
                else:
                    if document is None:
                        document = {} # Just treat it as an empty dictionary for now
                    mongo_fields = mongo_field.split('.')
                    result = document
                    for field in mongo_fields:
                        result = result.get(field, {})
                    if result == {}:
                        # We ended up with a nothing, return null
                        result = None
                    row_to_upload[column['name']] = result
                    if column.get('mode', 'NULLABLE') == 'REPEATED' and result is None:
                        row_to_upload[column['name']] = []
            else:
                if column.get('mode', 'NULLABLE') == 'REPEATED':
                    row_to_upload[column['name']] = [get_row(doc, column['fields']) for doc in document.get(mongo_field, [])]
                else:
                    row_to_upload[column['name']] = get_row(document.get(mongo_field, {}), column['fields'])
        except KeyError:
            if column.get('mode', 'NULLABLE') == 'REPEATED':
                row_to_upload[column['name']] = []
            else:
                row_to_upload[column['name']] = None # if the document doesn't have the field, we just send a null instead
    return row_to_upload

def upload_rows(bigquery_client, bigquery_table, rows_to_upload, collection_to_watch, operation_type):
    """Given the set of rows to upload, upload them to the specified bigquery table"""
    for row in rows_to_upload:
        row['time_archived'] = time.time()
        row['operationType'] = operation_type
    errors = bigquery_client.create_rows(bigquery_table, rows_to_upload, skip_invalid_rows=True)
    if errors:
        for i in xrange(len(errors)):
            # Add the actual failing document to the error so we can debug this at all
            errors[i]['document'] = rows_to_upload[errors[i]['index']]
            logging.error("Row insert failed: Updating table " + collection_to_watch['bigquery_table'] + " failed: " + str(errors[i]))
    return errors

def construct_schema(collection):
    """Given a Mongo collection, determine the schema that allows the most data to be uploaded to Bigquery
       Returns the schema, sets _id as the primary key always
       Column type is a dict of the form {name: str, type: str, mode: str, fields: str?}"""
    columns_dict = {}
    columns = []
    for row in collection.find():
        for field in row.keys():
            field_type = get_type(field, row[field])
            if field not in columns_dict.keys():
                columns_dict[field] = field_type
            else:
                union_type = unify_types(columns_dict[field], field_type)
                columns_dict[field] = union_type
    for field in sorted(columns_dict.keys()):
        # We sort the keys to make the constructed schema look nice
        # Possible failure modes up until this point:
        # Field is entirely empty arrays, type is undefined
        # Field is entirely empty objects
        # Field is invalid
        columns_dict[field] = remove_invalid_fields(columns_dict[field])
        if (columns_dict[field].get('type', 'INVALID') != 'INVALID' and
                not (columns_dict[field]['type'] == 'RECORD' and columns_dict[field]['fields'] == [])):
            columns.append(columns_dict[field])
    return columns

def remove_invalid_fields(field):
    """For a record definition, remove all the invalid fields, otherwise return itself
       Column type is a dict of the form {name: str, type: str, mode: str, fields: str?}"""
    if field.get('type', 'INVALID') == 'RECORD':
        field['fields'] = [remove_invalid_fields(subfield) for subfield in field['fields'] if subfield.get('type', 'INVALID') != 'INVALID']
        field['fields'] = [subfield for subfield in field['fields'] if subfield['type'] != 'RECORD' or subfield.get('fields', []) != []]
    return field

def get_bq_name(mongo_field):
    """Given a mongo field name, make one bigquery is happy with"""
    return ''.join([ch for ch in mongo_field if ch.isalnum() or ch == '_'])

def get_type(field, value):
    """Returns Bigquery type, mode, and record definition if any.
       Column type is a dict of the form {name: str, type: str, mode: str, fields: str?}"""
    field_type = {'name': get_bq_name(field), 'mode': 'NULLABLE', 'mongo_field': field}

    if field == '_id':
        field_type['type'] = "STRING"
        field_type['primary_key'] = True
    elif isinstance(value, datetime.datetime):
        field_type['type'] = "DATETIME"
    elif isinstance(value, (str, unicode)):
        field_type['type'] = "STRING"
    elif isinstance(value, bool):
        field_type['type'] = "BOOLEAN"
    elif isinstance(value, int):
        field_type['type'] = "INTEGER"
    elif isinstance(value, float):
        field_type['type'] = "FLOAT"
    elif isinstance(value, list):
        field_type['mode'] = "REPEATED"
        # we intentionally leave the type undefined for empty arrays here
        for i in value:
            field_type_2 = get_type(field, i)
            field_type_2['mode'] = "REPEATED"
            field_type = unify_types(field_type, field_type_2)
    elif isinstance(value, dict):
        field_type['type'] = "RECORD"
        field_type['fields'] = []
        for key in value:
            field_type['fields'].append(get_type(key, value[key]))
    else:
        # I give up
        field_type = {'name': get_bq_name(field), 'type': 'INVALID', 'mongo_field': field}
    return field_type

def unify_types(type1, type2):
    """Determine if two possible column types can agree, and return one that matches both. Assumes they have the same name already
       Column type is a dict of the form {name: str, type: str, mode: str, fields: str?}"""
    if type1 == type2:
        return type1
    if type1.get('mode', '') == 'REPEATED' and type2.get('mode', '') == 'REPEATED':
        # If it's an array, the type might not have yet been defined for empty arrays
        return type1 if type1.get('type', '') != '' else type2
    if type1.get('type', '') != 'RECORD' or type2.get('type', '') != 'RECORD' or type1['mode'] != type2['mode']:
        # In all mismatched cases except two records with the same name and mode, return an empty dict
        return {'name': type1['name'], 'type': 'INVALID', 'mongo_field': type1['mongo_field']}
    # We have two record definitions with the same name and mode. We have to find a unified set of fields for the two
    union_type = {'name': type1['name'], 'type': 'RECORD', 'mode': type1['mode'], 'fields': [], 'mongo_field': type1['mongo_field']}
    type_fields = {}
    for i in type1['fields']:
        type_fields[i['name']] = i
    for j in type2['fields']:
        if j['name'] in type_fields:
            union_type['fields'].append(unify_types(type_fields[j['name']], j))
            del type_fields[j['name']]
        else:
            union_type['fields'].append(j)
        if j['name'] == 'create_against':
            print j, union_type
    for remaining_key in type_fields:
        union_type['fields'].append(type_fields[remaining_key])
    return union_type

def watch_changes(collection_to_watch, mongo_client, backfill=False):
    """Main function for subprocesses that, given a mongo collection to watch, constructs a Bigquery table to upload
       the running changes to."""
    dryrun = False
    if 'dryrun' in collection_to_watch and collection_to_watch['dryrun']:
        dryrun = True
    client = MongoClient(mongo_client, connectTimeoutMS=30000, socketTimeoutMS=None, socketKeepAlive=True)
    if not backfill: # Don't log this message if we're reattempting with backfill logic on
        logging.info('Watching  %s -> %s', collection_to_watch['mongo_database'], collection_to_watch['mongo_collection'])
    watch_args = {'full_document': 'updateLookup'}
    last_doc_id = None
    if not backfill and 'backfill_full_collection' in collection_to_watch and 'lastChangeId' not in collection_to_watch and 'lastDocId' not in collection_to_watch:
        # First time set up for a collection, do a backfill before the first change
        logging.info("Performing initial backfill for %s", str(collection_to_watch['mongo_collection']))
        watch_changes(collection_to_watch, mongo_client, backfill=True)
    if 'lastChangeId' in collection_to_watch and not backfill:
        last_change_id = collection_to_watch['lastChangeId']
        watch_args['resume_after'] = last_change_id
    if 'lastDocId' in collection_to_watch:
        last_doc_id = collection_to_watch['lastDocId']
    try:
        bigquery_client = bigquery.Client.from_service_account_json(CONFIG['SERVICE_ACCOUNT_FILE'], project=CONFIG['BIGQUERY_PROJECT'])
        bigquery_dataset = bigquery_client.dataset(CONFIG['BIGQUERY_DATASET'])
        schema = []
        primary_key = None
        schema.append(bigquery.schema.SchemaField('time_archived', 'TIMESTAMP'))
        schema.append(bigquery.schema.SchemaField('operationType', 'STRING'))
        if 'columns' not in collection_to_watch or not collection_to_watch['columns']:
            logging.info("No schema found for %s, constructing one", collection_to_watch['mongo_collection'])
            columns = construct_schema(client[collection_to_watch['mongo_database']][collection_to_watch['mongo_collection']])
            client['config']['bigquery_archiver_settings'].find_one_and_update({'_id': collection_to_watch['_id']}, {'$set': {'columns': columns}})
            # Now that we've updated the schema, the parent process is going to cull us and restart. No point in continuing from here
            return

        for column in collection_to_watch['columns']:
            schema.append(build_schema_field(column))
            if 'primary_key' in column and column['primary_key']:
                primary_key = column['name']
        if not dryrun:
            bigquery_table_ref = bigquery_dataset.table(collection_to_watch['bigquery_table'])
            try:
                bigquery_table = bigquery_client.get_table(bigquery_table_ref)
            except Exception:
                logging.info("Table does not exist, creating it.")
                traceback.print_exc()
                bigquery_table = bigquery.Table(bigquery_table_ref, schema=schema)
                bigquery_client.create_table(bigquery_table)
            try:
                if primary_key:
                    # create a view that groups on primary key
                    bigquery_view_ref = bigquery_dataset.table(collection_to_watch['bigquery_table']+'_latest_view')
                    bigquery_view = bigquery.Table(bigquery_view_ref)
                    bigquery_view.view_query = """SELECT *
                    FROM (
                     SELECT *
                          , ROW_NUMBER() OVER(PARTITION BY """+primary_key+""" ORDER BY time_archived DESC) rank
                     FROM `""" + bigquery_table.project + "`." + bigquery_table.dataset_id + "."+ bigquery_table.table_id + """
                    )
                    WHERE rank=1 AND operationType != 'delete'"""
                    bigquery_view = bigquery_client.create_table(bigquery_view)
            except Exception:
                traceback.print_exc()
        try:
            changestream = client[collection_to_watch['mongo_database']][collection_to_watch['mongo_collection']].watch(**watch_args)
            rows_to_upload = []
            if not dryrun and backfill and (last_doc_id or collection_to_watch.get('backfill_full_collection')):
                find_filter = {}
                if last_doc_id:
                    find_filter['_id'] = {"$gte": bson.objectid.ObjectId.from_datetime(last_doc_id.generation_time)}
                for document in client[collection_to_watch['mongo_database']][collection_to_watch['mongo_collection']].find(find_filter):
                    rows_to_upload.append(get_row(document, collection_to_watch['columns']))
                    if len(rows_to_upload) == 10000:
                        logging.info("Backing up 10k rows for %s", collection_to_watch['bigquery_table'])
                        upload_rows(bigquery_client, bigquery_table, rows_to_upload, collection_to_watch, 'backfill')
                        rows_to_upload = []
                if rows_to_upload:
                    logging.info("Backing up remaining rows for %s", collection_to_watch['bigquery_table'])
                    upload_rows(bigquery_client, bigquery_table, rows_to_upload, collection_to_watch, 'backfill')
                    rows_to_upload = []
                backfill = False #only do a backfill once
            for change in changestream:
                # Change event format: https://docs.mongodb.com/manual/reference/change-events/
                # Types are insert, delete, replace, update - for now we're only dealing with insert
                last_change_id = change['_id']
                logging.debug(collection_to_watch['mongo_collection']+': '+str(change))
                if not dryrun:
                    if change['operationType'] in ['insert', 'replace', 'update']:
                        last_doc_id = change['fullDocument']['_id']
                        rows_to_upload.append(get_row(change['fullDocument'], collection_to_watch['columns']))
                    elif change['operationType'] == 'delete':
                        rows_to_upload.append({'_id': change['documentKey'], 'operationType': 'delete'})
                    logging.debug(collection_to_watch['mongo_collection']+' uploading: '+str(rows_to_upload))
                    # 10k is the hardcoded limit of bigquery rows in a single job
                    upload_tasks = [rows_to_upload[i:i+10000] for i in xrange(0, len(rows_to_upload), 10000)]
                    for task in upload_tasks:
                        upload_rows(bigquery_client, bigquery_table, task, collection_to_watch, change['operationType'])
                    rows_to_upload = []
                    client['config']['bigquery_archiver_settings'].find_one_and_update({'_id': collection_to_watch['_id']}, {'$set': {'lastChangeId': last_change_id, 'lastDocId': last_doc_id}})
        except Exception:
            traceback.print_exc()
            # Watch failed to catch up appropriately, do a backfill to catch up
            logging.info("Attempting backfill for failed watch")
            watch_changes(collection_to_watch, mongo_client, backfill=True)
    except Exception:
        traceback.print_exc()
        client = MongoClient(mongo_client)
        client['config']['bigquery_archiver_settings'].find_one_and_update({'_id': collection_to_watch['_id']}, {'$set': {'lastFailure': traceback.format_exc()}})

def main():
    """ Main function executed on startup."""
    client = MongoClient(CONFIG['MONGO_CLIENT'], connectTimeoutMS=30000, socketTimeoutMS=30000, socketKeepAlive=True)
    while True:
        monitoring_processes = []
        try:
            for collection_to_watch in client['config']['bigquery_archiver_settings'].find():
                process = multiprocessing.Process(target=watch_changes, args=(collection_to_watch, CONFIG['MONGO_CLIENT']))
                process.start()
                monitoring_processes += [process]
            for change in client['config']['bigquery_archiver_settings'].watch():
                if change['operationType'] != 'update' or ('lastChangeId' not in change['updateDescription']['updatedFields'].keys()):
                # As soon as we detect a non-checkpoint change (either to configs or as result of an error), start everything over
                    logging.debug(str(change))
                    logging.info('Restarting watch processes after update')
                    for running_process in monitoring_processes:
                        running_process.terminate()
                        running_process.join()
                    break
        except Exception:
            traceback.print_exc()
            logging.info('Restarting watch processes after main process failure')
            for running_process in monitoring_processes:
                running_process.terminate()
                running_process.join()

if __name__ == "__main__":
    main()
