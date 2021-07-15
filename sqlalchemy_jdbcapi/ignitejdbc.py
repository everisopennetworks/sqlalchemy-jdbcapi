from __future__ import absolute_import
from __future__ import unicode_literals

# from sqlalchemy import util
from sqlalchemy.engine.default import DefaultDialect

# from .base import MixedBinary, BaseDialect
from .base import BaseDialect


# from sqlalchemy.sql import sqltypes

# colspecs = util.update_copy(
#    DefaultDialect.colspecs, {sqltypes.LargeBinary: MixedBinary, },
# )


class IgniteJDBCDialect(BaseDialect, DefaultDialect):
    jdbc_db_name = "ignite"
    jdbc_driver_name = "org.apache.ignite.IgniteJdbcThinDriver"
    colspecs = DefaultDialect.colspecs

    def initialize(self, connection):
        super(IgniteJDBCDialect, self).initialize(connection)

    def _driver_kwargs(self):
        return {}

    def create_connect_args(self, url):
        if url is None:
            return
        # dialects expect jdbc url e.g.
        # "jdbc:oracle:thin@example.com:1521/db"
        # if sqlalchemy create_engine() url is passed e.g.
        # "oracle://scott:tiger@example.com/db"
        # it is parsed wrong
        # restore original url
        s: str = str(url)
        # get jdbc url
        jdbc_url: str = s.split("//", 1)[-1]
        # add driver information
        if not jdbc_url.startswith("jdbc"):
            jdbc_url = f"jdbc:ignite:thin://{jdbc_url}"
        kwargs = {
            "jclassname": self.jdbc_driver_name,
            "url": jdbc_url,
            # pass driver args via JVM System settings
            "driver_args": []
        }
        return (), kwargs

    def set_isolation_level(self, connection, level):
        if hasattr(connection, "connection"):
            dbapi_connection = connection.connection
        else:
            dbapi_connection = connection

        dbapi_connection.autocommit = False
        connection.rollback()


#    def _get_server_version_info(self, connection):
#
#        return 1, 0, 0


dialect = IgniteJDBCDialect
