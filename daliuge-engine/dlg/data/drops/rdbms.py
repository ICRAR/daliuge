#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
import contextlib
import importlib

from dlg.data.drops.data_base import DataDROP, logger
from dlg.exceptions import InvalidDropException
from dlg.data.io import ErrorIO
from dlg.meta import dlg_dict_param
from dlg.utils import prepare_sql


##
# @brief RDBMS
# @details A Drop allowing storage and retrieval from a SQL DB.
# @par EAGLE_START
# @param category RDBMS
# @param tag daliuge
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param dbmodule /String/ComponentParameter/NoPort/ReadWrite//False/False/Load path for python DB module
# @param dbtable /String/ComponentParameter/NoPort/ReadWrite//False/False/The name of the table to use
# @param vals {}/Json/ComponentParameter/NoPort/ReadWrite//False/False/Json encoded values dictionary used for INSERT. The keys of ``vals`` are used as the column names.
# @param condition /String/ComponentParameter/NoPort/ReadWrite//False/False/Condition for SELECT. For this the WHERE statement must be written using the "{X}" or "{}" placeholders
# @param selectVals {}/Json/ComponentParameter/NoPort/ReadWrite//False/False/Values for the WHERE statement
# @param dropclass dlg.data.drops.rdbms.RDBMSDrop/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name rdbms/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @par EAGLE_END
class RDBMSDrop(DataDROP):
    """
    A Drop that stores data in a table of a relational database
    """

    dbparams = dlg_dict_param("dbparams", {})

    def initialize(self, **kwargs):
        DataDROP.initialize(self, **kwargs)

        if "dbmodule" not in kwargs:
            raise InvalidDropException(
                self, '%r needs a "dbmodule" parameter' % (self,)
            )
        if "dbtable" not in kwargs:
            raise InvalidDropException(self, '%r needs a "dbtable" parameter' % (self,))

        # The DB-API 2.0 module
        dbmodname = kwargs.pop("dbmodule")
        self._db_drv = importlib.import_module(dbmodname)

        # The table this Drop points at
        self._db_table = kwargs.pop("dbtable")

        # Data store for reproducibility
        self._querylog = []

    def getIO(self):
        # This Drop cannot be accessed directly
        return ErrorIO()

    def _connection(self):
        return contextlib.closing(self._db_drv.connect(**self.dbparams))

    def _cursor(self, conn):
        return contextlib.closing(conn.cursor())

    def insert(self, vals: dict):
        """
        Inserts the values contained in the ``vals`` dictionary into the
        underlying table. The keys of ``vals`` are used as the column names.
        """
        with self._connection() as c:
            with self._cursor(c) as cur:
                # vals is a dictionary, its keys are the column names and its
                # values are the values to insert
                sql = "INSERT into %s (%s) VALUES (%s)" % (
                    self._db_table,
                    ",".join(vals.keys()),
                    ",".join(["{}"] * len(vals)),
                )
                sql, vals = prepare_sql(
                    sql, self._db_drv.paramstyle, list(vals.values())
                )
                logger.debug("Executing SQL with parameters: %s / %r", sql, vals)
                cur.execute(sql, vals)
                c.commit()

    def select(self, columns=None, condition=None, vals=()):
        """
        Returns the selected values from the table. Users can constrain the
        result set by specifying a list of ``columns`` to be returned (otherwise
        all table columns are returned) and a ``condition`` to be applied,
        in which case a list of ``vals`` to be applied as query parameters can
        also be given.
        """
        with self._connection() as c:
            with self._cursor(c) as cur:
                # Build up SQL with optional columns and conditions
                columns = columns or ("*",)
                sql = ["SELECT %s FROM %s" % (",".join(columns), self._db_table)]
                if condition:
                    sql.append(" WHERE ")
                    sql.append(condition)

                # Go, go, go!
                sql, vals = prepare_sql("".join(sql), self._db_drv.paramstyle, vals)
                logger.debug("Executing SQL with parameters: %s / %r", sql, vals)
                cur.execute(sql, vals)
                if cur.description:
                    ret = cur.fetchall()
                else:
                    ret = []
                self._querylog.append((sql, vals, ret))
                return ret

    @property
    def dataURL(self) -> str:
        return "rdbms://%s/%s/%r" % (
            self._db_drv.__name__,
            self._db_table,
            self._db_params,
        )

    # Override
    def generate_reproduce_data(self):
        return {"query_log": self._querylog}
