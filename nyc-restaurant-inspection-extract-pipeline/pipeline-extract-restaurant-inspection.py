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
            'name': 'camis', 'type': 'STRING', 'mode': 'REQUIRED'
        }, 
        {
            'name': 'dba', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'boro', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'building', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'street', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'zipcode', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'cuisine_description', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'inspection_date', 'type': 'DATETIME', 'mode': 'REQUIRED'
        },
        {
            'name': 'action', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'violation_code', 'type': 'STRING', 'mode': 'REQUIRED'
        },
        {
            'name': 'violation_description', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'critical_flag', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'score', 'type': 'NUMERIC', 'mode': 'NULLABLE'
        },
        {
            'name': 'grade', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'grade_date', 'type': 'DATETIME', 'mode': 'NULLABLE'
        },
        {
            'name': 'record_date', 'type': 'DATETIME', 'mode': 'REQUIRED'
        },
        {
            'name': 'inspection_type', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'latitude', 'type': 'FLOAT64', 'mode': 'REQUIRED'
        },
        {
            'name': 'longitude', 'type': 'FLOAT64', 'mode': 'REQUIRED'
        },
        {
            'name': 'community_board', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'council_district', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'census_tract', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'bin', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'bbl', 'type': 'STRING', 'mode': 'NULLABLE'
        },
        {
            'name': 'nta', 'type': 'STRING', 'mode': 'NULLABLE'
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
        return 'https://data.cityofnewyork.us/resource/43nn-pn8j.json?$query=' + urllib.parse.quote(query)

    def process(self, violationCode):
        import requests
        import math

        try:
            countQuery = 'SELECT COUNT(*) AS `count` \
                    WHERE (`inspection_date` >= "' + self.dataStartDate + '" :: floating_timestamp) \
                        AND (`inspection_date` < "' + self.dataEndDate + '" :: floating_timestamp) \
                        AND (`latitude` IS NOT NULL) \
                        AND (`longitude` IS NOT NULL) \
                        AND (`camis` IS NOT NULL) \
                        AND caseless_one_of(`violation_code`, "' + violationCode + '")'
            apiUrl = self.getApiUrl(countQuery)

            res = requests.get(apiUrl, headers=self.headers)
            res.raise_for_status()

            totalRecords = int(json.loads(res.text)[0]['count'])
            apiUrls = []

            if totalRecords > 0:

                apiQuery = 'SELECT `camis`, `dba`, `boro`, `building`, `street`, \
                            `zipcode`, `phone`, `cuisine_description`, `inspection_date`, \
                            `action`, `violation_code`, `violation_description`, `critical_flag`, \
                            `score`, `grade`, `grade_date`, `record_date`, `inspection_type`, \
                            `latitude`, `longitude`, `community_board`, `council_district`, \
                            `census_tract`, `bin`, `bbl`, `nta` \
                        WHERE (`inspection_date` >= "' + self.dataStartDate + '" :: floating_timestamp) \
                            AND (`inspection_date` < "' + self.dataEndDate + '" :: floating_timestamp) \
                            AND (`latitude` IS NOT NULL) \
                            AND (`longitude` IS NOT NULL) \
                            AND (`camis` IS NOT NULL) \
                            AND caseless_one_of(`violation_code`, "' + violationCode + '")'
                totalPages = math.ceil(totalRecords/self.pageSize)

                logging.info("Found violation code %s for %d rows with %d pages", violationCode, totalRecords, totalPages)

                for pageNumber in range(totalPages):
                    offset = pageNumber * self.pageSize
                    apiUrl = self.getApiUrl(apiQuery + ' LIMIT ' + str(self.pageSize) + ' OFFSET ' + str(offset)) 
                    apiUrls.append(apiUrl)

            logging.info("Total %d API URLs are generated for violation code %s", len(apiUrls), violationCode)

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
            'camis': self.get_string_value(record, 'camis'),
            'dba': self.get_string_value(record, 'dba'),
            'boro': self.get_string_value(record, 'boro'),
            'building': self.get_string_value(record, 'building'),
            'street': self.get_string_value(record, 'street'),
            'zipcode': self.get_string_value(record, 'zipcode'),
            'phone': self.get_string_value(record, 'phone'),
            'cuisine_description': self.get_string_value(record, 'cuisine_description'),
            'inspection_date': self.get_date_value(record, 'inspection_date'),
            'action': self.get_string_value(record, 'action'),
            'violation_code': self.get_string_value(record, 'violation_code'),
            'violation_description': self.get_string_value(record, 'violation_description'),
            'critical_flag': self.get_string_value(record, 'critical_flag'),
            'score': self.get_float_value(record, 'score'),
            'grade': self.get_string_value(record, 'grade'),
            'grade_date': self.get_date_value(record, 'grade_date'),
            'record_date': self.get_date_value(record, 'record_date'),
            'inspection_type': self.get_string_value(record, 'inspection_type'),
            'latitude': self.get_float_value(record, 'latitude'),
            'longitude': self.get_float_value(record, 'longitude'),
            'community_board': self.get_string_value(record, 'community_board'),
            'council_district': self.get_string_value(record, 'council_district'),
            'census_tract': self.get_string_value(record, 'census_tract'),
            'bin': self.get_string_value(record, 'bin'),
            'bbl': self.get_string_value(record, 'bbl'),
            'nta': self.get_string_value(record, 'nta')
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
        
        logging.info("Extract data with inspection_date >= %s && < %s", self.dataStartDate, self.dataEndDate)

        with beam.Pipeline(options=beam_options) as p:
            (p
            | 'Load focus violation codes' >> beam.io.ReadFromBigQuery(
                table=f'{self.GCLOUD_BIGQUERY_PROJECT_ID}:{self.GCLOUD_BIGQUERY_DATASET}.focus_violation_code',
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            | 'Extract violation code value' >> beam.Map(lambda lookup: lookup['violation_code'])
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