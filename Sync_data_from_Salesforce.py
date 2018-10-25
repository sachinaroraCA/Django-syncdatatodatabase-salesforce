from django.core.management.base import BaseCommand
from app2.serializers import *
from collections import OrderedDict
from integration.utils.models_for_serializers import get_model
from integration.utils import app_utils
from integration.models import ObjectSetting, SFObjectsMigration, SFConfig
from integration.sf_utils.sf_apis import SFConnectAPI
import datetime
import time
from integration.utils import pyodbc_util

# from background_task import background
import logging
# Get an instance of a logger
logger = logging.getLogger(__name__)


IGNORE_FIELDS = "MIGRATION_STATUS,TC_ERROR_MSG,SALESFORCE_ID,JOB,BATCH"
IGNORE_FIELDS_DB = 'tc_error_msg,salesforce_id,migration_status,job,batch'

#
@background(schedule=60)
def salesforce_to_db_in_backoround(id, sf_config_id):

    sf_obj = SFObjectsMigration.objects.get(pk=id)
    sf_config = SFConfig.objects.get(pk=sf_config_id)
    update_sf_obj_records_to_db(sf_config, sf_obj)
    print("salesforce to db sync executed..")
    sf_obj.sync_status = False
    sf_obj.save()


def update_sf_obj_records_to_db(sf_config, sf_obj):
    try:
        table = sf_obj.sf_obj_api
        sf_conn = SFConnectAPI(sf_config)
        sf_conn.salesforce_api_trace()

        connection = pyodbc_util.get_db_connection( server=sf_config.db_host,
                                                    database=sf_config.db_database,
                                                    username=sf_config.db_username,
                                                    password=sf_config.db_password,
                                                    port=sf_config.db_port )

        cursor = connection.cursor()
        query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'".format(table=str(table))
        columns =[]
        record_dict ={}
        cursor.execute(query)
        field_list = cursor.fetchall()

        ignore_fields =['tc_error_msg',
                        'salesforce_id',
                        'migration_status',
                        'job',
                        'batch']

        fields_string = ObjectSetting.objects.get(sf_obj=sf_obj).db_ignore_fields
        ignore_fields_db = fields_string.replace(' ','').split(',')
        ignore_fields.extend(list(ignore_fields_db))
        ignore_fields = [field.upper() for field in ignore_fields]
        for row in field_list:
            if str(row.COLUMN_NAME).upper() not in ignore_fields or str(row.COLUMN_NAME).upper() == 'ID':
                record_dict.update({row.COLUMN_NAME: row.DATA_TYPE})
                columns.append(row.COLUMN_NAME)

        sf_data_records = []

        print("SF query ready to execute..")

        index = 0
        idx = 0
        while index <= len(columns):
            if index < len(columns)-100:
                column_list = columns[index:index+100]
            else:
                column_list = columns[index:]
            sf_data_records.append(sf_conn.sf.query_all("SELECT {0} FROM {table} order by CREATEDDATE desc".format(",".join(column_list),
                                                                                                          table=str(table))))
            idx += 1
            index += 100

        for i in range(0, idx-1):
            for j in range(0,len(sf_data_records[i]["records"])):
                try:
                    sf_data_records[0]["records"][j].update(sf_data_records[i+1]["records"][j])
                except Exception as ex:
                    print(repr(ex))

    except Exception as ex:
        print("error while SF query...", ex[0]['message'])
        sf_obj.tc_error_msg = str(ex.content[0]['message'])
        sf_obj.tc_error_code =str(ex.content[0]['errorCode'])
        sf_obj.tc_error_date = datetime.datetime.now()
        sf_obj.tc_retry = '1'
        sf_obj.save()
        return "Error while fetching salesforce records to sync.."

    if sf_data_records:
        print("total records..", len(sf_data_records[0]["records"]))
        i = 0
        append_external_id = False
        update_records = []
        ext_id_dict = {}
        model_name = app_utils.to_class_name( table )
        model = get_model( model_name, app_name=sf_config.migration_app)

        sr = get_serializer_class( model )

        for obj_records in sf_data_records[0]["records"]:
            data = OrderedDict([(k.replace("__", "_").lower(), v) for k, v in obj_records.items()])
            data = OrderedDict([('salesforce_id', v) if k == 'id' else (k,v) for k, v in data.items()])

            for k,v in data.items():
                if k == "tc_tam_data_tam_external_c":
                    ext_id_dict["tc_tam_data_tam_external_c"] = v

            db_obj = model.objects.filter(salesforce_id=data['salesforce_id']).first()
            if db_obj:
                print('already exists')
                serializer = sr(db_obj, data=data, partial=True)
            else:
                print("saving as new object", i)
                serializer = sr(data=data)
            if "tc_tam_data_tam_external_c" in ext_id_dict and not ext_id_dict["tc_tam_data_tam_external_c"]:
                append_external_id = True
            else:
                append_external_id = False
            i = i+1

            if serializer.is_valid():
                serializer.save()
            else:
                print(repr(serializer.errors))

            if append_external_id:
                db_obj = model.objects.filter(salesforce_id=data['salesforce_id']).first()
                if db_obj:
                    update_records.append({'id':db_obj.salesforce_id,
                                           'TC_TAM_Data__TAM_External__c':db_obj.tc_tam_data_tam_external_c})

        if update_records:
            result = sf_conn.bulk_update_with_salesforce_bulk(table, update_records)
            # is_success = result[index][1]


    return 'success'

