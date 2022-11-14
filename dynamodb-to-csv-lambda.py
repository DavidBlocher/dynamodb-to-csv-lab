import csv
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import dateutil.tz

# Assign your aws clients/resources to use
ses_client = boto3.client('ses')
s3_resource = boto3.resource('s3')
dynamodb_resource = boto3.resource('dynamodb')

def lambda_handler(event, context):
    #create timestamps 
    def get_timestamps():
      eastern = dateutil.tz.gettz('US/Eastern')
      eastern_date = datetime.now(eastern)
      fmt = '%m-%d-%Y'
      date_format = eastern_date.strftime(fmt)
      time_format = eastern_date.strftime('%I:%M:%p')
    
      return {
        "date_format": str(date_format),
        "time_format": str(time_format)
      }
      
    # Reference your target table
    table = dynamodb_resource.Table('my-dynamo-table')
    
    # Here we get some timestamp attributes
    timestamps = get_timestamps()
    # Here we name our csv and append the date
    fileNameFormat = 'dynamodb_csv_snapshot{}'.format(
        timestamps.get("date_format"))
    csvFileName = '/tmp/{}.csv'.format(fileNameFormat)
    
    # Here we setup our dynamo table and its item limit
    response = table.scan(Limit=1000)
    
    if len(response['Items']) != 0:
        items = response['Items']
        # Here we get the keys of the first object in items.=
        keys = items[0].keys()
    
        for i in items:
            with open(csvFileName, 'a') as f:
                dict_writer = csv.DictWriter(f, keys)
                # Here we check to see if its the first write.
                if f.tell() == 0:
                    dict_writer.writeheader()
                    dict_writer.writerow(i)
                else:
                    dict_writer.writerow(i)
    
        # Save the csv file in S3
        s3Object = s3_resource.Object(
            'BUCKET_NAME', 'csv-files/{}'.format(fileNameFormat))
        s3Response = s3Object.put(Body=open(csvFileName, 'rb'))