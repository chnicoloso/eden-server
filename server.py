import os
from bottle import Bottle, request, hook, route, response, run
import pandas as pd
from dataUtils import top_n_crops_produced_at_point, top_n_production_points_for_crop, coordinates, interesting_points
import boto3
import botocore

BUCKET_NAME = 'eden-web'
ACTUAL_KEY = 'actual_production.csv'
PREDICTED_KEY = 'predicted_production.csv'

# Download local copy of RESOURCE from given AWS BUCKET.
def download_resource(bucket, resource):
    s3=boto3.client('s3')
    list=s3.list_objects(Bucket=BUCKET_NAME)['Contents']
    for key in list:
        s3.download_file(BUCKET_NAME, key['Key'], key['Key'])

actual = None
predicted = None

app = Bottle()

# Load the panda table
def _initialize():
    global actual
    global predicted

    # Fetch tables. Get the remote sources if not available locally.
    if (not os.path.isfile(ACTUAL_KEY)):
        download_resource(BUCKET_NAME, ACTUAL_KEY)
    if (not os.path.isfile(PREDICTED_KEY)):
        download_resource(BUCKET_NAME, PREDICTED_KEY)

    actual = pd.read_csv('./actual_production.csv')
    predicted = pd.read_csv('./predicted_production.csv')

    print('data ready') # :/
    return

@app.hook('after_request')
def enable_cors():
    '''Add headers to enable CORS'''
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Authorization, Origin, Accept, Content-Type, X-Requested-With'

@app.route('/top_points', method=['GET'])
def index():
    crop = request.query['crop']
    n = int(request.query['n'])
    table = request.query['table']

    if (table == 'predicted'):
        table = predicted
    elif (table == 'actual'):
        table = actual
    elif (table == 'interesting'):
        table = interesting_points(crop, n, actual, predicted)

    top_points = top_n_production_points_for_crop(crop, n, table)

    return {'data': top_points}

@app.route('/top_crops', method=['GET'])
def index():
    x = float(request.query['x'])
    y = float(request.query['y'])
    n = int(request.query['n'])

    top_crops_actual = top_n_crops_produced_at_point(x, y, n, actual)
    top_crops_predicted = top_n_crops_produced_at_point(x, y, n, predicted)

    return {'data': {'actual': top_crops_actual, 'predicted': top_crops_predicted}};

@app.route('/coordinates/all', method=['GET'])
def index():
    return {'data': coordinates(actual)}


if __name__ == "__main__":
    if (os.environ.get('APP_LOCATION') == 'heroku'):
        run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    else:
        run(host='localhost', port=8080, debug=True)

    _initialize()
