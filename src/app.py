from flask import Flask, send_file
import io
import boto3

app = Flask(__name__)

@app.route('/rain-graph-la')
def rain_graph_la():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='weather-etl-bucket-yanin', Key='plots/rain_severity_la.png')
    return send_file(io.BytesIO(obj['Body'].read()), mimetype='image/png')

@app.route('/rain-frequency-trends')
def rain_frequency_trends():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='weather-etl-bucket-yanin', Key='plots/rain_frequency_trends.png')
    return send_file(io.BytesIO(obj['Body'].read()), mimetype='image/png')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)