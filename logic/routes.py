import materialize_database
from flask import Flask, jsonify
import json

app = Flask("HDISED DW BACKEND")

db = materialize_database.database()


@app.route("/", methods=["GET"])
def index():
    return jsonify(
        {
            "app_name": "HDISED DATA WAREHOUSE - NGINX LOG PARSER",
            "version": "v0.0.1-alpha",
        }
    )


@app.route("/requests/all", methods=["GET"])
def get_all():
    db.refresh_all_items()
    data = db.all_items
    return jsonify(
        {
            "columns": [
                "Source IP",
                "Request timestamp",
                "Request HTTP method",
                "Request endpoint",
                "Request HTTP version",
                "Response code",
                "Response size (bytes)",
                "Client User-Agent",
            ],
            "data": data,
        }
    )


@app.route("/requests/count/by-ip", methods=["GET"])
def get_request_count_by_ip():
    return jsonify(
        {"columns": ["Source IP", "Request count"], "data": db.request_count_by_ip()}
    )


@app.route("/requests/count/average", methods=["GET"])
def get_average_request_count_per_ip():
    return jsonify(
        {"columns": ["Average request count"], "data": db.average_requests_count()}
    )


@app.route("/requests/count/most-requests", methods=["GET"])
def get_highest_request_ip():
    return jsonify(
        {
            "columns": ["Source IP", "Request count"],
            "data": db.highest_request_count_and_ip(),
        }
    )


@app.route("/responses/http-good/all", methods=["GET"])
def get_http_good_responses_list():
    return jsonify(
        {
            "columns": [
                "Source IP",
                "Request timestamp",
                "Request HTTP method",
                "Request endpoint",
                "Request HTTP version",
                "Response code",
                "Response size (bytes)",
                "Client User-Agent",
            ],
            "data": db.http_good_responses_list(),
        }
    )


@app.route("/responses/http-good/count/total", methods=["GET"])
def get_http_good_responses_count():
    return jsonify(
        {
            "columns": ["Response count"],
            "data": [db.http_good_responses_count()],
        }
    )


app.run()
