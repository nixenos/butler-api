import pg8000.native as postgres


class database:
    connection = postgres.Connection(
        user="materialize", host="localhost", port=6875, database="materialize"
    )
    all_items = connection.run("SELECT * FROM jsonified_hdised_dw_source;")

    def refresh_all_items(self):
        self.all_items = self.connection.run(
            "SELECT * FROM jsonified_hdised_dw_source;"
        )

    def average_requests_count(self):
        result = float.__int__(
            self.connection.run("SELECT * FROM average_request_count;")[0][0]
        )
        return result

    def request_count_by_ip(self):
        result = self.connection.run(
            "SELECT * FROM request_count_per_ip as src order by src.counts desc;"
        )
        return result

    def highest_request_count_and_ip(self):
        result = self.connection.run("SELECT * FROM highest_number_of_requests;")[0]
        return result

    def http_good_responses_list(self):
        result = self.connection.run("SELECT * from http_good_responses;")
        return result

    def http_good_responses_count(self):
        result = self.connection.run("SELECT count(*) from http_good_responses;")[0][0]
        return result

    def http_good_responses_list_by_ip(self, source_ip):
        print(source_ip)
        result = self.connection.run(
            f"SELECT * FROM http_good_responses WHERE http_good_responses.source_ip LIKE '%{source_ip}%';",
        )
        return result

    def http_good_responses_count_by_ip(self, source_ip):
        result = self.connection.run(
            f"SELECT http_good_responses.source_ip, count(*) FROM http_good_responses WHERE http_good_responses.source_ip LIKE '%{source_ip}%' GROUP BY http_good_responses.source_ip;"
        )
        return result

    def http_good_responses_count_by_ip_all(self):
        result = self.connection.run(
            "SELECT http_good_responses.source_ip, count(http_good_responses.request_timestamp) FROM http_good_responses group by http_good_responses.source_ip;"
        )
        return result

    def http_redirect_responses_list(self):
        result = self.connection.run("SELECT * from http_redirect_responses;")
        return result

    def http_redirect_responses_count(self):
        result = self.connection.run("SELECT count(*) from http_redirect_responses;")[
            0
        ][0]
        return result

    def http_redirect_responses_list_by_ip(self, source_ip):
        result = self.connection.run(
            f"SELECT * FROM http_redirect_responses WHERE http_redirect_responses.source_ip LIKE '%{source_ip}%' group by http_redirect_responses.source_ip;"
        )
        return result

    def http_redirect_responses_count_by_ip(self, source_ip):
        result = self.connection.run(
            f"SELECT http_redirect_responses.source_ip, count(*) FROM http_redirect_responses WHERE http_redirect_responses.source_ip LIKE '%{source_ip}%' GROUP BY http_redirect_responses.source_ip;",
            src_ip=source_ip,
        )
        return result

    def http_redirect_responses_count_by_ip_all(self):
        result = self.connection.run(
            "SELECT http_redirect_responses.source_ip, count(http_redirect_responses.request_timestamp) FROM http_redirect_responses group by http_redirect_responses.source_ip;"
        )
        return result

    def http_client_error_responses_list(self):
        result = self.connection.run("SELECT * from http_redirect_responses;")
        return result

    def http_redirect_responses_count(self):
        result = self.connection.run(
            "SELECT count(*) from http_client_error_responses;"
        )[0][0]
        return result

    def http_client_error_responses_list_by_ip(self, source_ip):
        result = self.connection.run(
            f"SELECT * FROM http_client_error_responses WHERE http_client_error_responses.source_ip LIKE '%{source_ip}%';"
        )
        return result

    def http_client_error_responses_count_by_ip(self, source_ip):
        result = self.connection.run(
            f"SELECT http_client_error_responses.source_ip, count(*) FROM http_client_error_responses WHERE http_redirect_client_error.source_ip LIKE '%{source_ip}%';"
        )
        return result

    def http_client_error_responses_count_by_ip_all(self):
        result = self.connection.run(
            "SELECT http_client_error_responses.source_ip, count(http_client_error_responses.request_timestamp) FROM http_client_error_responses group by http_client_error_responses.source_ip;"
        )
        return result

    def http_server_error_responses_list(self):
        result = self.connection.run("SELECT * from http_server_error_responses;")
        return result

    def http_server_error_responses_count(self):
        result = self.connection.run(
            "SELECT count(*) from http_server_error_responses;"
        )[0][0]
        return result

    def http_server_error_responses_list_by_ip(self, source_ip):
        result = self.connection.run(
            f"SELECT * FROM http_server_error_responses WHERE http_server_error_responses.source_ip LIKE '%{source_ip}%';"
        )
        return result

    def http_server_error_responses_count_by_ip(self, source_ip):
        result = self.connection.run(
            f"SELECT http_server_error_responses.source_ip, count(*) FROM http_server_error_responses WHERE http_server_error_responses.source_ip LIKE '%{source_ip}%' group by http_server_error_responses.source_ip;"
        )
        return result

    def http_server_error_responses_count_by_ip_all(self):
        result = self.connection.run(
            "SELECT http_server_error_responses.source_ip, count(http_redirect_responses.request_timestamp) FROM http_redirect_responses group by http_redirect_responses.source_ip;"
        )
        return result

    def count_all_records(self):
        result = self.connection.run("SELECT count(*) FROM jsonified_hdised_dw_source;")
        return result[0][0]

    def list_bot_requests(self):
        result = self.connection.run("SELECT * FROM bot_requests_filtered;")
        return result

    def count_bot_requests(self):
        result = self.connection.run("SELECT COUNT(*) FROM bot_requests_filtered;")
        return result[0][0]

    def list_human_requests(self):
        result = self.connection.run("SELECT * FROM human_requests_filtered;")
        return result

    def count_human_requests(self):
        result = self.connection.run("SELECT COUNT(*) FROM human_requests_filtered;")
        return result[0][0]

    def count_requests_by_endpoint_desc(self):
        result = self.connection.run(
            "select * from requests_count_by_endpoint order by requests_count_by_endpoint.counts desc;"
        )
        return result

    def count_requests_by_endpoint_desc_limit(self, limit):
        result = self.connection.run(
            f"select * from requests_count_by_endpoint order by requests_count_by_endpoint.counts desc limit {limit};"
        )
        return result


# db = database()
