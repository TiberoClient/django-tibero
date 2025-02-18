from collections import namedtuple

from django.db import models
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.introspection import FieldInfo as BaseFieldInfo
from django.db.backends.base.introspection import TableInfo as BaseTableInfo

from .base import Database

FieldInfo = namedtuple(
    "FieldInfo", BaseFieldInfo._fields + ("is_autofield", "is_json", "comment")
)
TableInfo = namedtuple("TableInfo", BaseTableInfo._fields + ("comment",))

# TODO: 아래 상수들은 pyodbc에서 제공안하는 상수들입니다. 추측으로는 mssql odbc에서 제공하는 커스텀
#       타입인 것으로 추측됩니다. 티베로에 맞게 코드를 고치기
SQL_AUTOFIELD = -777555
SQL_BIGAUTOFIELD = -777444
SQL_SMALLAUTOFIELD = -777333
SQL_TIMESTAMP_WITH_TIMEZONE = -155

class DatabaseIntrospection(BaseDatabaseIntrospection):
    cache_bust_counter = 1

    # TODO: data_types_reverse의 값들은 mssql-django에서 그대로 가져왔습니다.
    #       Tibero에 맞게 수정할 때 base.py의 DatabaseWrapper.data_type에 있는 타입과
    #       일대일 매칭이 되도록 하기
    # Maps type objects to Django Field types.
    data_types_reverse = {
        SQL_AUTOFIELD: 'AutoField',
        SQL_BIGAUTOFIELD: 'BigAutoField',
        SQL_SMALLAUTOFIELD: 'SmallAutoField',
        Database.SQL_BIGINT: 'BigIntegerField',
        # Database.SQL_BINARY:            ,
        Database.SQL_BIT: 'BooleanField',
        Database.SQL_CHAR: 'CharField',
        Database.SQL_DECIMAL: 'DecimalField',
        Database.SQL_DOUBLE: 'FloatField',
        Database.SQL_FLOAT: 'FloatField',
        Database.SQL_GUID: 'TextField',
        Database.SQL_INTEGER: 'IntegerField',
        Database.SQL_LONGVARBINARY: 'BinaryField',
        # Database.SQL_LONGVARCHAR:       ,
        Database.SQL_NUMERIC: 'DecimalField',
        Database.SQL_REAL: 'FloatField',
        Database.SQL_SMALLINT: 'SmallIntegerField',
        Database.SQL_SS_TIME2: 'TimeField',
        Database.SQL_TINYINT: 'SmallIntegerField',
        Database.SQL_TYPE_DATE: 'DateField',
        Database.SQL_TYPE_TIME: 'TimeField',
        Database.SQL_TYPE_TIMESTAMP: 'DateTimeField',
        SQL_TIMESTAMP_WITH_TIMEZONE: 'DateTimeField',
        Database.SQL_VARBINARY: 'BinaryField',
        Database.SQL_VARCHAR: 'TextField',
        Database.SQL_WCHAR: 'CharField',
        Database.SQL_WLONGVARCHAR: 'TextField',
        Database.SQL_WVARCHAR: 'TextField',
    }

    def get_field_type(self, data_type, description):
        # TODO: Tibero7에 맞게 수정하기
        if data_type == Database.NUMBER:
            precision, scale = description[4:6]
            if scale == 0:
                if precision > 11:
                    return (
                        "BigAutoField"
                        if description.is_autofield
                        else "BigIntegerField"
                    )
                elif 1 < precision < 6 and description.is_autofield:
                    return "SmallAutoField"
                elif precision == 1:
                    return "BooleanField"
                elif description.is_autofield:
                    return "AutoField"
                else:
                    return "IntegerField"
            elif scale == -127:
                return "FloatField"
        # TODO: Tibero7에 맞게 수정하기
        elif data_type == Database.NCLOB and description.is_json:
            return "JSONField"

        return super().get_field_type(data_type, description)

    def get_table_list(self, cursor):
        """Return a list of table and view names in the current database."""
        cursor.execute(
            """
            SELECT
                user_tables.table_name,
                't',
                user_tab_comments.comments
            FROM user_tables
            LEFT OUTER JOIN
                user_tab_comments
                ON user_tab_comments.table_name = user_tables.table_name
            WHERE
                NOT EXISTS (
                    SELECT 1
                    FROM user_mviews
                    WHERE user_mviews.mview_name = user_tables.table_name
                )
            UNION ALL
            SELECT view_name, 'v', NULL FROM user_views
            UNION ALL
            SELECT mview_name, 'v', NULL FROM user_mviews
        """
        )
        return [
            TableInfo(self.identifier_converter(row[0]), row[1], row[2])
            for row in cursor.fetchall()
        ]

    def get_table_description(self, cursor, table_name):
        """
        Return a description of the table with the DB-API cursor.description
        interface.
        """
        # A default collation for the given table/view/materialized view.
        cursor.execute(
            """
            SELECT user_tables.default_collation
            FROM user_tables
            WHERE
                user_tables.table_name = UPPER(%s) AND
                NOT EXISTS (
                    SELECT 1
                    FROM user_mviews
                    WHERE user_mviews.mview_name = user_tables.table_name
                )
            UNION ALL
            SELECT user_views.default_collation
            FROM user_views
            WHERE user_views.view_name = UPPER(%s)
            UNION ALL
            SELECT user_mviews.default_collation
            FROM user_mviews
            WHERE user_mviews.mview_name = UPPER(%s)
            """,
            [table_name, table_name, table_name],
        )
        row = cursor.fetchone()
        default_table_collation = row[0] if row else ""
        # user_tab_columns gives data default for columns
        cursor.execute(
            """
            SELECT
                user_tab_cols.column_name,
                user_tab_cols.data_default,
                CASE
                    WHEN user_tab_cols.collation = %s
                    THEN NULL
                    ELSE user_tab_cols.collation
                END collation,
                CASE
                    WHEN user_tab_cols.char_used IS NULL
                    THEN user_tab_cols.data_length
                    ELSE user_tab_cols.char_length
                END as display_size,
                CASE
                    WHEN user_tab_cols.identity_column = 'YES' THEN 1
                    ELSE 0
                END as is_autofield,
                CASE
                    WHEN EXISTS (
                        SELECT  1
                        FROM user_json_columns
                        WHERE
                            user_json_columns.table_name = user_tab_cols.table_name AND
                            user_json_columns.column_name = user_tab_cols.column_name
                    )
                    THEN 1
                    ELSE 0
                END as is_json,
                user_col_comments.comments as col_comment
            FROM user_tab_cols
            LEFT OUTER JOIN
                user_col_comments ON
                user_col_comments.column_name = user_tab_cols.column_name AND
                user_col_comments.table_name = user_tab_cols.table_name
            WHERE user_tab_cols.table_name = UPPER(%s)
            """,
            [default_table_collation, table_name],
        )
        field_map = {
            column: (
                display_size,
                default.rstrip() if default and default != "NULL" else None,
                collation,
                is_autofield,
                is_json,
                comment,
            )
            for (
                column,
                default,
                collation,
                display_size,
                is_autofield,
                is_json,
                comment,
            ) in cursor.fetchall()
        }
        self.cache_bust_counter += 1
        cursor.execute(
            "SELECT * FROM {} WHERE ROWNUM < 2 AND {} > 0".format(
                self.connection.ops.quote_name(table_name), self.cache_bust_counter
            )
        )
        description = []
        for desc in cursor.description:
            name = desc[0]
            (
                display_size,
                default,
                collation,
                is_autofield,
                is_json,
                comment,
            ) = field_map[name]
            name %= {}  # oracledb, for some reason, doubles percent signs.
            description.append(
                FieldInfo(
                    self.identifier_converter(name),
                    desc[1],
                    display_size,
                    desc[3],
                    desc[4] or 0,
                    desc[5] or 0,
                    *desc[6:],
                    default,
                    collation,
                    is_autofield,
                    is_json,
                    comment,
                )
            )
        return description

    def identifier_converter(self, name):
        """Identifier comparison is case insensitive under Oracle."""
        return name.lower()

    def get_sequences(self, cursor, table_name, table_fields=()):
        cursor.execute(
            """
            SELECT
                user_tab_identity_cols.sequence_name,
                user_tab_identity_cols.column_name
            FROM
                user_tab_identity_cols,
                user_constraints,
                user_cons_columns cols
            WHERE
                user_constraints.constraint_name = cols.constraint_name
                AND user_constraints.table_name = user_tab_identity_cols.table_name
                AND cols.column_name = user_tab_identity_cols.column_name
                AND user_constraints.constraint_type = 'P'
                AND user_tab_identity_cols.table_name = UPPER(%s)
            """,
            [table_name],
        )
        # Oracle allows only one identity column per table.
        row = cursor.fetchone()
        if row:
            return [
                {
                    "name": self.identifier_converter(row[0]),
                    "table": self.identifier_converter(table_name),
                    "column": self.identifier_converter(row[1]),
                }
            ]
        # To keep backward compatibility for AutoFields that aren't Oracle
        # identity columns.
        for f in table_fields:
            if isinstance(f, models.AutoField):
                return [{"table": table_name, "column": f.column}]
        return []

    def get_relations(self, cursor, table_name):
        """
        Return a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all foreign keys in the given table.
        """
        table_name = table_name.upper()
        cursor.execute(
            """
    SELECT ca.column_name, cb.table_name, cb.column_name
    FROM   user_constraints, USER_CONS_COLUMNS ca, USER_CONS_COLUMNS cb
    WHERE  user_constraints.table_name = %s AND
           user_constraints.constraint_name = ca.constraint_name AND
           user_constraints.r_constraint_name = cb.constraint_name AND
           ca.position = cb.position""",
            [table_name],
        )

        return {
            self.identifier_converter(field_name): (
                self.identifier_converter(rel_field_name),
                self.identifier_converter(rel_table_name),
            )
            for field_name, rel_table_name, rel_field_name in cursor.fetchall()
        }

    def get_primary_key_columns(self, cursor, table_name):
        cursor.execute(
            """
            SELECT
                cols.column_name
            FROM
                user_constraints,
                user_cons_columns cols
            WHERE
                user_constraints.constraint_name = cols.constraint_name AND
                user_constraints.constraint_type = 'P' AND
                user_constraints.table_name = UPPER(%s)
            ORDER BY
                cols.position
            """,
            [table_name],
        )
        return [self.identifier_converter(row[0]) for row in cursor.fetchall()]

    def get_constraints(self, cursor, table_name):
        """
        Retrieve any constraints or keys (unique, pk, fk, check, index) across
        one or more columns.
        """
        constraints = {}
        # Loop over the constraints, getting PKs, uniques, and checks
        cursor.execute(
            """
            SELECT
                user_constraints.constraint_name,
                LISTAGG(LOWER(cols.column_name), ',')
                    WITHIN GROUP (ORDER BY cols.position),
                CASE user_constraints.constraint_type
                    WHEN 'P' THEN 1
                    ELSE 0
                END AS is_primary_key,
                CASE
                    WHEN user_constraints.constraint_type IN ('P', 'U') THEN 1
                    ELSE 0
                END AS is_unique,
                CASE user_constraints.constraint_type
                    WHEN 'C' THEN 1
                    ELSE 0
                END AS is_check_constraint
            FROM
                user_constraints
            LEFT OUTER JOIN
                user_cons_columns cols
                ON user_constraints.constraint_name = cols.constraint_name
            WHERE
                user_constraints.constraint_type = ANY('P', 'U', 'C')
                AND user_constraints.table_name = UPPER(%s)
            GROUP BY user_constraints.constraint_name, user_constraints.constraint_type
            """,
            [table_name],
        )
        for constraint, columns, pk, unique, check in cursor.fetchall():
            constraint = self.identifier_converter(constraint)
            constraints[constraint] = {
                "columns": columns.split(","),
                "primary_key": pk,
                "unique": unique,
                "foreign_key": None,
                "check": check,
                "index": unique,  # All uniques come with an index
            }
        # Foreign key constraints
        cursor.execute(
            """
            SELECT
                cons.constraint_name,
                LISTAGG(LOWER(cols.column_name), ',')
                    WITHIN GROUP (ORDER BY cols.position),
                LOWER(rcols.table_name),
                LOWER(rcols.column_name)
            FROM
                user_constraints cons
            INNER JOIN
                user_cons_columns rcols
                ON rcols.constraint_name = cons.r_constraint_name AND rcols.position = 1
            LEFT OUTER JOIN
                user_cons_columns cols
                ON cons.constraint_name = cols.constraint_name
            WHERE
                cons.constraint_type = 'R' AND
                cons.table_name = UPPER(%s)
            GROUP BY cons.constraint_name, rcols.table_name, rcols.column_name
            """,
            [table_name],
        )
        for constraint, columns, other_table, other_column in cursor.fetchall():
            constraint = self.identifier_converter(constraint)
            constraints[constraint] = {
                "primary_key": False,
                "unique": False,
                "foreign_key": (other_table, other_column),
                "check": False,
                "index": False,
                "columns": columns.split(","),
            }
        # Now get indexes
        cursor.execute(
            """
            SELECT
                ind.index_name,
                LOWER(ind.index_type),
                LOWER(ind.uniqueness),
                LISTAGG(LOWER(cols.column_name), ',')
                    WITHIN GROUP (ORDER BY cols.column_position),
                LISTAGG(cols.descend, ',') WITHIN GROUP (ORDER BY cols.column_position)
            FROM
                user_ind_columns cols, user_indexes ind
            WHERE
                cols.table_name = UPPER(%s) AND
                NOT EXISTS (
                    SELECT 1
                    FROM user_constraints cons
                    WHERE ind.index_name = cons.index_name
                ) AND cols.index_name = ind.index_name
            GROUP BY ind.index_name, ind.index_type, ind.uniqueness
            """,
            [table_name],
        )
        for constraint, type_, unique, columns, orders in cursor.fetchall():
            constraint = self.identifier_converter(constraint)
            constraints[constraint] = {
                "primary_key": False,
                "unique": unique == "unique",
                "foreign_key": None,
                "check": False,
                "index": True,
                "type": "idx" if type_ == "normal" else type_,
                "columns": columns.split(","),
                "orders": orders.split(","),
            }
        return constraints