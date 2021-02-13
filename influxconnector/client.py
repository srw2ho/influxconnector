from influxdb import InfluxDBClient
import json


class InfluxClient(object):
    """[summary]
    Arguments:
        object {[type]} -- [description]
    """

    def connect(self, host='localhost', port=8086, username='', password='', database='default'):
        """ Connect to InfluxDB server

        Keyword Arguments:
            host {str} -- Hostname of InfluxDB server (default: {HOST})
            port {[type]} -- InfluxDB server port (default: {PORT})
            username {[type]} -- InfluxDB login username (default: {USERNAME})
            password {[type]} -- InfluxDB login password (default: {PASSWORD})
        """
        self.database = database
        self._db = InfluxDBClient(host, port, username, password, database=database, retries=0)

        # check if db exists (create otherwise)
        dblist = [db['name'] for db in self._db.get_list_database()]
        if self.database not in dblist:
            self._db.create_database(self.database)

    def write(self, data, protocol='json'):
        """ Writes data to InfluxDB database

        Arguments:
            data {json} -- Information to store in InfluxDB (needs to be in appropriate InfluxDB format)
            database {str} -- Name of the database to insert data into
        """
        self._db.write_points(data, database=self.database, protocol=protocol)

    def read(self, query, database):
        """ Read values from InfluxDB database

        Arguments:
            query {str} -- InfluxDB query string
        """
        self._db.switch_database(database)
        return self._db.query(query)

    def alter_retention_policy(self, name='autogen', database = 'default', duration=None, replication=None, default=None, shard_duration=None):
        """ alter retention policies

        Arguments:
        """
        self._db.alter_retention_policy(name, database, duration, replication, default, shard_duration)
    
    def get_list_retention_policies(self, database=None):
        """ get retention policies
        Arguments: database
        """
        return self._db.get_list_retention_policies(database)