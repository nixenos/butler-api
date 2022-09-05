from logic.database import materialize_database
from flask import Flask, jsonify
from flask_swagger import swagger
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
        {
            "columns": ["Source IP", "Request count"],
            "data": db.request_count_by_ip(),
        }
    )


@app.route("/requests/count/average", methods=["GET"])
def get_average_request_count_per_ip():
    return jsonify(
        {"columns": ["Average request count"], "data": [db.average_requests_count()]}
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


@app.route("/responses/http-good/all/count", methods=["GET"])
def get_http_good_responses_count():
    return jsonify(
        {
            "columns": ["Response count"],
            "data": [db.http_good_responses_count()],
        }
    )


@app.route("/responses/http-good/all/count/by-ip", methods=["GET"])
def get_http_good_responses_count_by_ip():
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": [db.http_good_responses_count_by_ip_all()],
        }
    )


@app.route("/responses/http-good/<ip>")
def get_http_good_responses_for_ip(ip):
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
            "data": db.http_good_responses_list_by_ip(ip),
        }
    )


@app.route("/responses/http-good/<ip>/count")
def get_http_good_responses_count_for_ip(ip):
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": db.http_good_responses_count_by_ip(ip)[0],
        }
    )


@app.route("/responses/http-redirect/all", methods=["GET"])
def get_http_redirect_responses_list():
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
            "data": db.http_redirect_responses_list(),
        }
    )


@app.route("/responses/http-redirect/all/count", methods=["GET"])
def get_http_redirect_responses_count():
    return jsonify(
        {
            "columns": ["Response count"],
            "data": [db.http_redirect_responses_count()],
        }
    )


@app.route("/responses/http-redirect/all/count/by-ip", methods=["GET"])
def get_http_redirect_responses_count_by_ip():
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": [db.http_redirect_responses_count_by_ip_all()],
        }
    )


@app.route("/responses/http-redirect/<ip>")
def get_http_redirect_responses_for_ip(ip):
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
            "data": db.http_redirect_responses_list_by_ip(ip),
        }
    )


@app.route("/responses/http-redirect/<ip>/count")
def get_http_redirect_responses_count_for_ip(ip):
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": db.http_redirect_responses_count_by_ip(ip)[0],
        }
    )


@app.route("/responses/http-error-client/all", methods=["GET"])
def get_http_client_error_responses_list():
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
            "data": db.http_client_error_responses_list(),
        }
    )


@app.route("/responses/http-error-client/all/count", methods=["GET"])
def get_http_client_error_responses_count():
    return jsonify(
        {
            "columns": ["Response count"],
            "data": [db.http_client_error_responses_count()],
        }
    )


@app.route("/responses/http-client_error/all/count/by-ip", methods=["GET"])
def get_http_client_error_responses_count_by_ip():
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": [db.http_client_error_responses_count_by_ip_all()],
        }
    )


@app.route("/responses/http-error-client/<ip>")
def get_http_client_error_responses_for_ip(ip):
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
            "data": db.http_client_error_responses_list_by_ip(ip),
        }
    )


@app.route("/responses/http-error-client/<ip>/count")
def get_http_client_error_responses_count_for_ip(ip):
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": db.http_client_error_responses_count_by_ip(ip)[0],
        }
    )


@app.route("/responses/http-error-server/all", methods=["GET"])
def get_http_server_error_responses_list():
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
            "data": db.http_server_error_responses_list(),
        }
    )


@app.route("/responses/http-error-server/all/count", methods=["GET"])
def get_http_server_error_responses_count():
    return jsonify(
        {
            "columns": ["Response count"],
            "data": [db.http_server_error_responses_count()],
        }
    )


@app.route("/responses/http-error-server/all/count/by-ip", methods=["GET"])
def get_http_server_error_responses_count_by_ip():
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": [db.http_server_error_responses_count_by_ip_all()],
        }
    )


@app.route("/responses/http-error-server/<ip>")
def get_http_server_error_responses_for_ip(ip):
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
            "data": db.http_server_error_responses_list_by_ip(ip),
        }
    )


@app.route("/responses/http-error-server/<ip>/count")
def get_http_server_error_responses_count_for_ip(ip):
    return jsonify(
        {
            "columns": ["Source IP", "Response count"],
            "data": db.http_server_error_responses_count_by_ip(ip)[0],
        }
    )


@app.route("/requests/all/count")
def get_all_request_count():
    return jsonify(
        {"columns": ["Number of requests"], "data": [db.count_all_records()]}
    )


@app.route("/requests/bots/count")
def get_bots_requests_count():
    return jsonify(
        {"columns": ["Number of requests"], "data": [db.count_bot_requests()]}
    )


@app.route("/requests/humans/count")
def get_humans_requests_count():
    return jsonify(
        {"columns": ["Number of requests"], "data": [db.count_human_requests()]}
    )


@app.route("/requests/bots")
def get_bots_requests_list():
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
            "data": db.list_bot_requests(),
        }
    )


@app.route("/requests/humans")
def get_humans_requests_list():
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
            "data": db.list_human_requests(),
        }
    )


@app.route("/requests/per-endpoint/desc")
def get_requests_list_by_endpoint_desc():
    return jsonify(
        {
            "columns": ["Request endpoint", "Request HTTP method", "Requests count"],
            "data": db.count_requests_by_endpoint_desc(),
        }
    )


@app.route("/requests/per-endpoint/desc/<limit>")
def get_requests_list_by_endpoint_desc_limit(limit):
    return jsonify(
        {
            "columns": ["Request endpoint", "Request HTTP method", "Requests count"],
            "data": db.count_requests_by_endpoint_desc_limit(limit),
        }
    )


@app.route("/specs")
def swagger_spec():
    return jsonify(swagger(app))


app.run()
