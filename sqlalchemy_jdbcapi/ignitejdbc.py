from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.sql import sqltypes
from sqlalchemy import util, exc
from .base import MixedBinary, BaseDialect

colspecs = util.update_copy(
    OracleDialect.colspecs, {sqltypes.LargeBinary: MixedBinary,},
)


class IgniteJDBCDialect(BaseDialect, OracleDialect):
    jdbc_db_name = "ignite"
    jdbc_driver_name = "org.apache.ignite.IgniteJdbcThinDriver"
    colspecs = colspecs

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
            jdbc_url = f"jdbc:ignite:thin:@{jdbc_url}"
        kwargs = {
            "jclassname": self.jdbc_driver_name,
            "url": jdbc_url,
            # pass driver args via JVM System settings
            "driver_args": []
        }
        return ((), kwargs)

    def _get_server_version_info(self, connection):

        return 3, 6, 18


dialect = IgniteJDBCDialect
