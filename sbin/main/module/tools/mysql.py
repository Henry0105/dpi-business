# coding=utf-8
import pymysql


class MySQLClient:
    def __init__(self, **connection_properties):
        self.host = connection_properties['host']
        self.port = connection_properties['port']
        self.user = connection_properties['user']
        self.password = connection_properties['password']
        self.db = connection_properties['db']
        self.connection = pymysql.connect(host=self.host,
                                          port=self.port,
                                          user=self.user,
                                          password=self.password,
                                          db=self.db,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def query_rows(self, query):
        """
        :param query:
        :return: read rows
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def update_rows(self, query):
        """
        :param query:
        :return: update rows count
        """
        with self.connection.cursor() as cursor:
            # update rows
            cursor.execute(query)
            self.connection.commit()
            return cursor.rowcount

    def close(self):
        self.connection.close()