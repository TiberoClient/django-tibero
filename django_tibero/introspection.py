from collections import namedtuple

from django.db import models, ProgrammingError
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.introspection import FieldInfo as BaseFieldInfo
from django.db.backends.base.introspection import TableInfo as BaseTableInfo

from .utils import remove_parentheses_numbers

FieldInfo = namedtuple(
    "FieldInfo", BaseFieldInfo._fields + ("is_autofield", "is_json", "comment")
)
TableInfo = namedtuple("TableInfo", BaseTableInfo._fields + ("comment",))

class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type objects to Django Field types.
    data_types_reverse = {
        "DATE": "DateField",
        "DOUBLE PRECISION": "FloatField",
        "BLOB": "BinaryField",
        "CHAR": "CharField",
        "CLOB": "TextField",
        "INTERVAL DAY TO SECOND": "DurationField",
        "NCHAR": "CharField",
        "NCLOB": "TextField",
        "NVARCHAR": "CharField",
        "DECIMAL": "DecimalField",
        "TIMESTAMP": "DateTimeField",
        "VARCHAR": "CharField",
    }

    def get_field_type(self, data_type, description):
        # TODO: Tibero7에 맞게 수정하기
        if data_type == "DECIMAL":
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
        # TODO: 나중에 user_json_columns view가 Tibero에서 지원되면 json기능 추가하기
        #       그 전까지는 django에서 json이 올바르게 작동하는 방법을 차지 못했습니다.

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

        # Tibero는 oracle과 다르게 default_collation column을 지원하지 않습니다. 대신 nls_session_parameters에서
        # 필요한 collation 정보를 가져옵니다.
        cursor.execute("SELECT VALUE FROM nls_session_parameters WHERE parameter = 'NLS_COMP'")
        default_table_collation = cursor.fetchone()[0]

        # user_tab_columns gives data default for columns
        cursor.execute(
            """
            SELECT
                user_tab_cols.column_name,
                user_tab_cols.data_default,
                CASE
                    WHEN user_tab_cols.char_used IS NULL
                    THEN user_tab_cols.data_length
                    ELSE user_tab_cols.char_length
                END as display_size,
                CASE 
                    WHEN EXISTS (
                        SELECT 1
                        FROM user_sequences 
                        WHERE user_sequences.sequence_name LIKE '%%' || user_tab_cols.table_name || '_' || user_tab_cols.column_name || '_SQ'
                    )
                    THEN 1
                    ELSE 0
                END AS is_autofield,
                0 as is_json,
                user_col_comments.comments as col_comment
            FROM user_tab_cols
            LEFT OUTER JOIN
                user_col_comments ON
                user_col_comments.column_name = user_tab_cols.column_name AND
                user_col_comments.table_name = user_tab_cols.table_name
            WHERE user_tab_cols.table_name = UPPER(%s)
            """,
            [table_name],
        )
        field_map = {
            column: (
                display_size,
                default.rstrip() if default and default != "NULL" else None,
                default_table_collation,
                is_autofield,
                is_json,
                comment,
            )
            for (
                column,
                default,
                display_size,
                is_autofield,
                is_json,
                comment,
            ) in cursor.fetchall()
        }


        # Each row has the following columns:
        #     table_cat
        #     table_schem
        #     table_name
        #     column_name       3
        #     data_type         4
        #     type_name         5
        #     column_size       6
        #     buffer_length
        #     decimal_digits    7
        #     num_prec_radix
        #     nullable          8
        #     remarks
        #     column_def        10
        #     sql_data_type
        #     sql_datetime_sub
        #     char_octet_length
        #     ordinal_position
        #     is_nullable: One of SQL_NULLABLE, SQL_NO_NULLS, SQL_NULLABLE_UNKNOWN.
        cursor.columns(table=table_name.upper())
        column_metadata = list(cursor)

        # 유저가 존재하지 않는 테이블 이름을 이 메서드에 전달할 수도 있기 때문에 존재하지 않으면 일부러
        # 에러를 발생시킨다.
        if len(column_metadata) == 0:
            raise ProgrammingError("42S02", f"Specified schema object was not found: {table_name}")

        description = []
        for desc in column_metadata:
            name = desc[3]
            type_name = desc[5]
            if "INTERVAL" in type_name or "TIMESTAMP" in type_name:
                type_name = remove_parentheses_numbers(type_name)

            (
                display_size,
                default,
                collation,
                is_autofield,
                is_json,
                comment,
            ) = field_map[name]
            description.append(
                FieldInfo(
                    self.identifier_converter(name), # name
                    type_name,                       # type_name
                    display_size,                    # display_size
                    desc[6],                         # internal_size
                    desc[6] or 0,                    # precision
                    desc[8] or 0,                    # scale
                    desc[10],                         # null_ok
                    default,
                    collation,
                    is_autofield,
                    is_json,
                    comment,
                )
            )
        return description

    def identifier_converter(self, name):
        """Identifier comparison is case insensitive under Tibero."""
        return name.lower()

    # TODO: Tibero6_FS06_CS2005에서 지원안하는 user_tab_identity_cols를 이용하는 sql을 변경해야 합니다.
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
        # Tibero allows only one identity column per table.
        row = cursor.fetchone()
        if row:
            return [
                {
                    "name": self.identifier_converter(row[0]),
                    "table": self.identifier_converter(table_name),
                    "column": self.identifier_converter(row[1]),
                }
            ]
        # To keep backward compatibility for AutoFields that aren't Tibero
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

        # Oracle의 경우 user_col_columns의 position의 start number는 1이지만 Tibero의
        # start number는 0입니다.
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
                ON rcols.constraint_name = cons.r_constraint_name AND rcols.position = 0
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

        # index에 대한 정보를 얻기위한 오라클 쿼리는 아래와 같습니다. 그런데 WHERE 조건 아래 NOT EXISTS (...) 의 결과가
        # 티베로랑 다르게 반환됩니다. 오라클의 경우 foreign key constraint에 unique constraint가 있어도 index_name이
        # null값이지만 티베로는 index_name이 비어있지 않습니다. 이로 인해 서로 다른 결과를 반환합니다.
        # 이는 schema.tests.SchemaTests.test_alter_field_o2o_to_fk 테스트에서 확인 가능합니다.
        #
        # cursor.execute(
        #     """
        #     SELECT
        #         ind.index_name,
        #         LOWER(ind.index_type),
        #         LOWER(ind.uniqueness),
        #         LISTAGG(LOWER(cols.column_name), ',')
        #             WITHIN GROUP (ORDER BY cols.column_position),
        #         LISTAGG(cols.descend, ',') WITHIN GROUP (ORDER BY cols.column_position)
        #     FROM
        #         user_ind_columns cols, user_indexes ind
        #     WHERE
        #         cols.table_name = UPPER(%s) AND
        #         NOT EXISTS (
        #             SELECT 1
        #             FROM user_constraints cons
        #             WHERE ind.index_name = cons.index_name
        #         ) AND cols.index_name = ind.index_name
        #     GROUP BY ind.index_name, ind.index_type, ind.uniqueness
        #     """,
        #     [table_name],
        # )

        # django app에서 만든 index는 DB가 만드는 임의의 index name이 아니라 명시적인 이름을 가지고 있습니다.
        # 그리고 쿼리를 보면 django app이 명시적으로 만든 index를 찾는 것으로 추정하고 있습니다. 그래서 쿼리를
        # 아래와 같이 고쳤습니다.
        #
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
                NOT REGEXP_LIKE(ind.index_name, '^_.+CON[0-9]+$')
                AND cols.index_name = ind.index_name
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