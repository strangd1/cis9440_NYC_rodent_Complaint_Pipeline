import sys
import json
import logging
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

TARGET_TABLE_SCHEMA = {
    'fields': [
        {
            'name': 'unique_key', 'type': 'STRING', 'mode': 'REQUIRED'#, 'max_length': 20
        }, 
        {
            'name': 'created_date', 'type': 'DATETIME', 'mode': 'REQUIRED'
        },
        {
            'name': 'closed_date', 'type': 'DATETIME', 'mode': 'NULLABLE'
        },
        # {
        #     'name': 'agency', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        # {
        #     'name': 'agency_name', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        {
            'name': 'complaint_type', 'type': 'STRING', 'mode': 'REQUIRED'#, 'max_length': 50
        },
        {
            'name': 'descriptor', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'location_type', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'incident_zip', 'type': 'STRING', 'mode': 'NULLABLE'#, 'max_length': 20
        },
        {
            'name': 'incident_address', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'street_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'cross_street_1', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'cross_street_2', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'intersection_street_1', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'intersection_street_2', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'address_type', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'landmark', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'facility_type', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'due_date', 'type': 'DATETIME', 'mode': 'NULLABLE'
        },
        {
            'name': 'resolution_description', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'resolution_action_updated_date', 'type': 'DATETIME', 'mode': 'NULLABLE'
        },
        {
            'name': 'community_board', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'bbl', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'borough', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'x_coordinate_state_plane', 'type': 'NUMERIC', 'mode': 'NULLABLE'#, 'precision': 38, 'scale': 14
        },
        {
            'name': 'y_coordinate_state_plane', 'type': 'NUMERIC', 'mode': 'NULLABLE'#, 'precision': 38, 'scale': 14
        },
        {
            'name': 'open_data_channel_type', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'park_facility_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'park_borough', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        # {
        #     'name': 'vehicle_type', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        # {
        #     'name': 'taxi_company_borough', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        # {
        #     'name': 'taxi_pick_up_location', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        # {
        #     'name': 'bridge_highway_name', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        # {
        #     'name': 'bridge_highway_direction', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        # {
        #     'name': 'road_ramp', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        # {
        #     'name': 'bridge_highway_segment', 'type': 'STRING', 'mode': 'NULLABLE'
        # },
        {
            'name': 'latitude', 'type': 'FLOAT64', 'mode': 'REQUIRED'#, 'precision': 38, 'scale': 14
        },
        {
            'name': 'longitude', 'type': 'FLOAT64', 'mode': 'REQUIRED'#, 'precision': 38, 'scale': 14
        },
        {
            'name': 'compute_zip', 'type': 'STRING', 'mode': 'NULLABLE'#, 'precision': 38, 'scale': 14
        },
        {
            'name': 'compute_borough_boundaries', 'type': 'STRING', 'mode': 'NULLABLE'#, 'precision': 38, 'scale': 14
        }
    ]
}

# https://stackoverflow.com/questions/66689429/how-to-get-data-from-an-api-using-apache-beam-dataflow
class GenerateNYCOpenDataAPIUrls(beam.DoFn):
    def __init__(self, headers, dataStartDate, dataEndDate, pageSize):
        self.headers = headers
        self.dataStartDate = dataStartDate
        self.dataEndDate = dataEndDate
        self.pageSize = pageSize

    def getApiUrl(self, query):
        import urllib.parse
        return 'https://data.cityofnewyork.us/resource/erm2-nwe9.json?$query=' + urllib.parse.quote(query)

    def process(self, complaintType):
        import requests
        import math

        try:
            countQuery = 'SELECT COUNT(*) AS `count` \
                WHERE (`created_date` >= "' + self.dataStartDate + '" :: floating_timestamp) \
                    AND (`created_date` < "' + self.dataEndDate + '" :: floating_timestamp) \
                    AND (`latitude` IS NOT NULL) \
                    AND (`longitude` IS NOT NULL) \
                    AND (`unique_key` IS NOT NULL) \
                    AND caseless_eq(`complaint_type`, "' + complaintType + '")'
            apiUrl = self.getApiUrl(countQuery)

            res = requests.get(apiUrl, headers=self.headers)
            res.raise_for_status()

            totalRecords = int(json.loads(res.text)[0]['count'])
            apiUrls = []

            if totalRecords > 0:

                apiQuery = 'SELECT `unique_key`, `created_date`, `closed_date`, `complaint_type`, \
                        `descriptor`, `location_type`, `incident_zip`, `incident_address`, `street_name`, \
                        `cross_street_1`, `cross_street_2`, `intersection_street_1`, `intersection_street_2`, \
                        `address_type`, `city`, `landmark`, `facility_type`, `status`, `due_date`, `resolution_description`, \
                        `resolution_action_updated_date`, `community_board`, `bbl`, `borough`, `x_coordinate_state_plane`, `y_coordinate_state_plane`, \
                        `open_data_channel_type`, `park_facility_name`, `park_borough`, `latitude`, `longitude`, \
                        `:@computed_region_efsh_h5xi`, `:@computed_region_yeji_bk3q` \
                    WHERE (`created_date` >= "' + self.dataStartDate + '" :: floating_timestamp) \
                        AND (`created_date` < "' + self.dataEndDate + '" :: floating_timestamp) \
                        AND (`latitude` IS NOT NULL) \
                        AND (`longitude` IS NOT NULL) \
                        AND (`unique_key` IS NOT NULL) \
                        AND caseless_eq(`complaint_type`, "' + complaintType + '")'
                totalPages = math.ceil(totalRecords/self.pageSize)

                logging.info("Found complaint type %s for %d rows with %d pages", complaintType, totalRecords, totalPages)

                for pageNumber in range(totalPages):
                    offset = pageNumber * self.pageSize
                    apiUrl = self.getApiUrl(apiQuery + ' LIMIT ' + str(self.pageSize) + ' OFFSET ' + str(offset)) 
                    apiUrls.append(apiUrl)

            logging.info("Total %d API URLs are generated for complaint type %s", len(apiUrls), complaintType)

            yield apiUrls
            
        except requests.HTTPError as message:
            logging.error(message)
            yield []


class CallNYCOpenDataAPI(beam.DoFn):
    def __init__(self, headers):
        self.headers = headers

    def process(self, apiUrl):
        import requests

        try:
            res = requests.get(apiUrl, headers=self.headers)
            res.raise_for_status()

            result = json.loads(res.text)
            logging.info("Found API response for %d rows", len(result))

            yield result
            
        except requests.HTTPError as message:
            logging.error(message)
            yield []


class MapAPIResponseToBigQueryRecord(beam.DoFn):
    def __init__(self, dateTimeFormat):
        self.dateTimeFormat = dateTimeFormat

    def get_string_value(self, record, key):
        if key in record and record[key] is not None:
            return record[key]
        else:
            return None
        
    def get_date_value(self, record, key):
        from datetime import datetime

        if key in record and record[key] is not None:
            return datetime.strptime(record[key], self.dateTimeFormat)
        else:
            return None
        
    def get_float_value(self, record, key):
        if key in record and record[key] is not None:
            try:
                return float(record[key])
            except:
                return None
        else:
            return None

    def process(self, record):
        out = {
            'unique_key': self.get_string_value(record, 'unique_key'),
            'created_date': self.get_date_value(record, 'created_date'),
            'closed_date': self.get_date_value(record, 'closed_date'),
            'complaint_type': self.get_string_value(record, 'complaint_type'),
            'descriptor': self.get_string_value(record, 'descriptor'),
            'location_type': self.get_string_value(record, 'location_type'),
            'incident_zip': self.get_string_value(record, 'incident_zip'),
            'incident_address': self.get_string_value(record, 'incident_address'),
            'street_name': self.get_string_value(record, 'street_name'),
            'cross_street_1': self.get_string_value(record, 'cross_street_1'),
            'cross_street_2': self.get_string_value(record, 'cross_street_2'),
            'intersection_street_1': self.get_string_value(record, 'intersection_street_1'),
            'intersection_street_2': self.get_string_value(record, 'intersection_street_2'),
            'address_type': self.get_string_value(record, 'address_type'),
            'city': self.get_string_value(record, 'city'),
            'landmark': self.get_string_value(record, 'landmark'),
            'facility_type': self.get_string_value(record, 'facility_type'),
            'status': self.get_string_value(record, 'status'),
            'due_date': self.get_date_value(record, 'due_date'),
            'resolution_description': self.get_string_value(record, 'resolution_description'),
            'resolution_action_updated_date': self.get_date_value(record, 'resolution_action_updated_date'),
            'community_board': self.get_string_value(record, 'community_board'),
            'bbl': self.get_string_value(record, 'bbl'),
            'borough': self.get_string_value(record, 'borough'),
            'x_coordinate_state_plane': self.get_float_value(record, 'x_coordinate_state_plane'),
            'y_coordinate_state_plane': self.get_float_value(record, 'y_coordinate_state_plane'),
            'open_data_channel_type': self.get_string_value(record, 'open_data_channel_type'),
            'park_facility_name': self.get_string_value(record, 'park_facility_name'),
            'park_borough': self.get_string_value(record, 'park_borough'),
            'latitude': self.get_float_value(record, 'latitude'),
            'longitude': self.get_float_value(record, 'longitude'),
            'compute_zip': self.get_string_value(record, ':@computed_region_efsh_h5xi'),
            'compute_borough_boundaries': self.get_string_value(record, ':@computed_region_yeji_bk3q')
        }

        return [out]
    



class Program():

    GCLOUD_BIGQUERY_PROJECT_ID: str
    GCLOUD_BIGQUERY_DATASET: str
    NYC_OPENDATA_API_KEY: str

    dataStartDate: str
    dataEndDate: str
    destWriteDisposition: beam.io.BigQueryDisposition

    def init_variables(self, mode):
        from datetime import datetime, timedelta
        from dateutil import tz

        utc_tz = tz.gettz('UTC')
        nyc_tz = tz.gettz('America/New_York')

        utc_now = datetime.utcnow().replace(tzinfo=utc_tz)

        one_day = timedelta(days=1)
        ny_today = utc_now.astimezone(nyc_tz).date()
        ny_yesterday = ny_today - one_day

        self.dataEndDate = ny_today.strftime(DATE_TIME_FORMAT)
        
        if mode == 'snapshot':
            self.destWriteDisposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
            self.dataStartDate = '2016-01-01T00:00:00'
        else:
            self.destWriteDisposition = beam.io.BigQueryDisposition.WRITE_APPEND
            self.dataStartDate = ny_yesterday.strftime(DATE_TIME_FORMAT)

    def main(self, argv=None):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--project',
            dest='project',
            required=True,
            help='Google BigQuery project ID')
        parser.add_argument(
            '--dataset',
            dest='dataset',
            required=True,
            help='Google BigQuery destination dataset name')
        parser.add_argument(
            '--dest_table',
            dest='dest_table',
            required=True,
            help='Google BigQuery destination table name')
        parser.add_argument(
            '--nyc_opendata_apikey',
            dest='nyc_opendata_apikey',
            required=True,
            help='NYC Open Data API Key')
        parser.add_argument(
            '--mode',
            dest='mode',
            required=True,
            help='snapshot = refresh all the data / increment = daily increment load')
        
        known_args, pipeline_args = parser.parse_known_args(argv)
        
        self.GCLOUD_BIGQUERY_PROJECT_ID = known_args.project
        self.GCLOUD_BIGQUERY_DATASET = known_args.dataset
        self.NYC_OPENDATA_API_KEY = known_args.nyc_opendata_apikey

        self.init_variables(known_args.mode)

        beam_options = PipelineOptions(sys.argv[1:])

        dest_table_spec = bigquery.TableReference(
            projectId=self.GCLOUD_BIGQUERY_PROJECT_ID,
            datasetId=self.GCLOUD_BIGQUERY_DATASET,
            tableId=known_args.dest_table)
        
        logging.info("Extract data with created_date >= %s && < %s", self.dataStartDate, self.dataEndDate)

        with beam.Pipeline(options=beam_options) as p:
            (p
            | 'Load focus complaint types' >> beam.io.ReadFromBigQuery(
                table=f'{self.GCLOUD_BIGQUERY_PROJECT_ID}:{self.GCLOUD_BIGQUERY_DATASET}.focus_complaint_type',
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            | 'Extract complaint type value' >> beam.Map(lambda lookup: lookup['complaint_type'])
            | 'Generate NYC Open Data API pagination URLs' >> beam.ParDo(GenerateNYCOpenDataAPIUrls(
                {'X-App-Token': self.NYC_OPENDATA_API_KEY, 'Accept': 'application/json'},
                self.dataStartDate,
                self.dataEndDate,
                1000))
            | 'Flat-map API URL' >> beam.FlatMap(lambda urls: urls)
            | 'Request NYC Open Data API' >> beam.ParDo(CallNYCOpenDataAPI(
                {'X-App-Token': self.NYC_OPENDATA_API_KEY, 'Accept': 'application/json'}))
            | 'Flat-map API response' >> beam.FlatMap(lambda records: records)
            | 'Map API response to entity' >> beam.ParDo(MapAPIResponseToBigQueryRecord(DATE_TIME_FORMAT))
            | 'Save to BigQuery' >> beam.io.WriteToBigQuery(
                dest_table_spec,
                schema=TARGET_TABLE_SCHEMA,
                write_disposition=self.destWriteDisposition,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    program = Program()
    program.main()