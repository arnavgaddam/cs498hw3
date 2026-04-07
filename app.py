from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference
from flask import Flask, request, jsonify

app = Flask(__name__)

MONGO_URI = "mongodb+srv://python:1234@cs498.tzi6gan.mongodb.net/?appName=cs498"

client = MongoClient(MONGO_URI)
db = client['ev_db']
cars = db['vehicles']

@app.route('/insert-fast', methods=['POST'])
def insert_fast():
    payload = request.json
    column = cars.with_options(write_concern=WriteConcern(w=1))
    result = column.insert_one(payload)
    return jsonify({"id": str(result.inserted_id)}), 201

@app.route('/insert-safe', methods=['POST'])
def insert_safe():
    payload = request.json
    column = cars.with_options(write_concern=WriteConcern(w='majority'))
    result = column.insert_one(payload)
    # need to avoid crash
    return jsonify({"id": str(result.inserted_id)}), 201

@app.route('/count-tesla-primary', methods=['GET'])
def count_tesla():
    payload = cars.with_options(read_preference=ReadPreference.PRIMARY)
    count = payload.count_documents({"Make": "TESLA"})
    return jsonify({"count": count})

@app.route('/count-bmw-secondary', methods=['GET'])
def count_bmw():
    payload = cars.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    count = payload.count_documents({"Make": "BMW"})
    return jsonify({"count": count})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=32720)

