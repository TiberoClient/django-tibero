import datetime
import uuid
from functools import lru_cache

from django.conf import settings
from django.db import NotSupportedError
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.ddl_references import Statement, Table
from django.db.backends.utils import split_tzname_delta, strip_quotes, truncate_name
from django.db.models import AutoField, Exists, ExpressionWrapper, Lookup
from django.db.models.expressions import RawSQL
from django.db.models.sql.where import WhereNode
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.regex_helper import _lazy_re_compile

from .base import Database


class DatabaseOperations(BaseDatabaseOperations):
    # Tibero uses NUMBER(5), NUMBER(11), and NUMBER(19) for integer fields.
    # SmallIntegerField uses NUMBER(11) instead of NUMBER(5), which is used by
    # SmallAutoField, to preserve backward compatibility.
    integer_field_ranges = {
        "SmallIntegerField": (-99999999999, 99999999999),
        "IntegerField": (-99999999999, 99999999999),
        "BigIntegerField": (-9999999999999999999, 9999999999999999999),
        "PositiveBigIntegerField": (0, 9999999999999999999),
        "PositiveSmallIntegerField": (0, 99999999999),
        "PositiveIntegerField": (0, 99999999999),
        "SmallAutoField": (-99999, 99999),
        "AutoField": (-99999999999, 99999999999),
        "BigAutoField": (-9999999999999999999, 9999999999999999999),
    }
    set_operators = {**BaseDatabaseOperations.set_operators, "difference": "MINUS"}

    # TODO: colorize this SQL code with style.SQL_KEYWORD(), etc.
#     _sequence_reset_sql = """
# DECLARE
#     table_value integer;
#     seq_value integer;
#     seq_name user_tab_identity_cols.sequence_name%%TYPE;
# BEGIN
#     BEGIN
#         SELECT sequence_name INTO seq_name FROM user_tab_identity_cols
#         WHERE  table_name = '%(table_name)s' AND
#                column_name = '%(column_name)s';
#         EXCEPTION WHEN NO_DATA_FOUND THEN
#             seq_name := '%(no_autofield_sequence_name)s';
#     END;
#
#     SELECT NVL(MAX(%(column)s), 0) INTO table_value FROM %(table)s;
#     SELECT NVL(last_number - cache_size, 0) INTO seq_value FROM user_sequences
#            WHERE sequence_name = seq_name;
#     WHILE table_value > seq_value LOOP
#         EXECUTE IMMEDIATE 'SELECT "'||seq_name||'".nextval%(suffix)s'
#         INTO seq_value;
#     END LOOP;
# END;
# """

    _sequence_reset_sql = """
    DECLARE
        table_value integer;
        seq_value integer;
    BEGIN
        SELECT NVL(MAX(%(column)s), 0) INTO table_value FROM %(table)s;
        SELECT NVL(last_number - cache_size, 0) INTO seq_value FROM user_sequences
               WHERE sequence_name = '%(sequence)s';
        WHILE table_value > seq_value LOOP
            SELECT "%(sequence)s".nextval INTO seq_value FROM dual;
        END LOOP;
    END;
    """

    # Tibero doesn't support string without precision; use the max string size.
    cast_char_field_without_max_length = "NVARCHAR2(2000)"
    cast_data_types = {
        "AutoField": "NUMBER(11)",
        "BigAutoField": "NUMBER(19)",
        "SmallAutoField": "NUMBER(5)",
        "TextField": cast_char_field_without_max_length,
    }

    def cache_key_culling_sql(self):
        cache_key = self.quote_name("cache_key")
        return (
            f"SELECT {cache_key} "
            f"FROM %s "
            f"ORDER BY {cache_key} OFFSET %%s ROWS FETCH FIRST 1 ROWS ONLY"
        )

    # EXTRACT format cannot be passed in parameters.
    _extract_format_re = _lazy_re_compile(r"[A-Z_]+")

    def date_extract_sql(self, lookup_type, sql, params):
        extract_sql = f"TO_CHAR({sql}, %s)"
        extract_param = None
        if lookup_type == "week_day":
            # TO_CHAR(field, 'D') returns an integer from 1-7, where 1=Sunday.
            extract_param = "D"
        elif lookup_type == "iso_week_day":
            extract_sql = f"TO_CHAR({sql} - 1, %s)"
            extract_param = "D"
        elif lookup_type == "week":
            # IW = ISO week number
            extract_param = "IW"
        elif lookup_type == "quarter":
            extract_param = "Q"
        elif lookup_type == "iso_year":
            extract_param = "IYYY"
        else:
            lookup_type = lookup_type.upper()
            if not self._extract_format_re.fullmatch(lookup_type):
                raise ValueError(f"Invalid loookup type: {lookup_type!r}")
            # https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/EXTRACT-datetime.html
            return f"EXTRACT({lookup_type} FROM {sql})", params
        return extract_sql, (*params, extract_param)

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        # https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ROUND-and-TRUNC-Date-Functions.html
        trunc_param = None
        if lookup_type in ("year", "month"):
            trunc_param = lookup_type.upper()
        elif lookup_type == "quarter":
            trunc_param = "Q"
        elif lookup_type == "week":
            trunc_param = "IW"
        else:
            return f"TRUNC({sql})", params
        return f"TRUNC({sql}, %s)", (*params, trunc_param)

    # Oracle crashes with "ORA-03113: end-of-file on communication channel"
    # if the time zone name is passed in parameter. Use interpolation instead.
    # https://groups.google.com/forum/#!msg/django-developers/zwQju7hbG78/9l934yelwfsJ
    # This regexp matches all time zone names from the zoneinfo database.
    _tzname_re = _lazy_re_compile(r"^[\w/:+-]+$")

    def _prepare_tzname_delta(self, tzname):
        tzname, sign, offset = split_tzname_delta(tzname)
        return f"{sign}{offset}" if offset else tzname

    def _convert_sql_to_tz(self, sql, params, tzname):
        if not (settings.USE_TZ and tzname):
            return sql, params
        if not self._tzname_re.match(tzname):
            raise ValueError("Invalid time zone name: %s" % tzname)
        # Convert from connection timezone to the local time, returning
        # TIMESTAMP WITH TIME ZONE and cast it back to TIMESTAMP to strip the
        # TIME ZONE details.
        if self.connection.timezone_name != tzname:
            from_timezone_name = self.connection.timezone_name
            to_timezone_name = self._prepare_tzname_delta(tzname)
            return (
                f"CAST((FROM_TZ({sql}, '{from_timezone_name}') AT TIME ZONE "
                f"'{to_timezone_name}') AS TIMESTAMP)",
                params,
            )
        return sql, params

    def datetime_cast_date_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"TRUNC({sql})", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        # Since `TimeField` values are stored as TIMESTAMP change to the
        # default date and convert the field to the specified timezone.
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        convert_datetime_sql = (
            f"TO_TIMESTAMP(CONCAT('1900-01-01 ', TO_CHAR({sql}, 'HH24:MI:SS.FF')), "
            f"'YYYY-MM-DD HH24:MI:SS.FF')"
        )
        return (
            f"CASE WHEN {sql} IS NOT NULL THEN {convert_datetime_sql} ELSE NULL END",
            (*params, *params),
        )

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        if lookup_type == "second":
            # Truncate fractional seconds.
            return f"FLOOR(EXTRACT(SECOND FROM {sql}))", params
        return self.date_extract_sql(lookup_type, sql, params)

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        # https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ROUND-and-TRUNC-Date-Functions.html
        trunc_param = None
        if lookup_type in ("year", "month"):
            trunc_param = lookup_type.upper()
        elif lookup_type == "quarter":
            trunc_param = "Q"
        elif lookup_type == "week":
            trunc_param = "IW"
        elif lookup_type == "hour":
            trunc_param = "HH24"
        elif lookup_type == "minute":
            trunc_param = "MI"
        elif lookup_type == "day":
            return f"TRUNC({sql})", params
        else:
            # Cast to DATE removes sub-second precision.
            return f"CAST({sql} AS DATE)", params
        return f"TRUNC({sql}, %s)", (*params, trunc_param)

    def time_extract_sql(self, lookup_type, sql, params):
        if lookup_type == "second":
            # Truncate fractional seconds.
            return f"FLOOR(EXTRACT(SECOND FROM {sql}))", params
        return self.date_extract_sql(lookup_type, sql, params)

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        # The implementation is similar to `datetime_trunc_sql` as both
        # `DateTimeField` and `TimeField` are stored as TIMESTAMP where
        # the date part of the later is ignored.
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        trunc_param = None
        if lookup_type == "hour":
            trunc_param = "HH24"
        elif lookup_type == "minute":
            trunc_param = "MI"
        elif lookup_type == "second":
            # Cast to DATE removes sub-second precision.
            return f"CAST({sql} AS DATE)", params
        return f"TRUNC({sql}, %s)", (*params, trunc_param)

    # Oracle Backend와 비교해서 코드 구현이 많이 다른 함수이다.
    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        # if internal_type in ["JSONField", "TextField"]:
        #     converters.append(self.convert_textfield_value)
        if internal_type == "SmallIntegerField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "IntegerField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "BigIntegerField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "PositiveBigIntegerField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "PositiveSmallIntegerField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "PositiveIntegerField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "SmallAutoField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "AutoField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "BigAutoField":
            converters.append(self.convert_integerfield_value)
        elif internal_type == "BooleanField":
            converters.append(self.convert_booleanfield_value)
        elif internal_type == "DateTimeField":
            if settings.USE_TZ:
                converters.append(self.convert_datetimefield_value)
        elif internal_type == "DateField":
            converters.append(self.convert_datefield_value)
        elif internal_type == "TimeField":
            converters.append(self.convert_timefield_value)
        elif internal_type == "UUIDField":
            converters.append(self.convert_uuidfield_value)
        # Tibero stores empty strings as null. If the field accepts the empty
        # string, undo this to adhere to the Django convention of using
        # the empty string instead of null.
        if expression.output_field.empty_strings_allowed:
            converters.append(
                self.convert_empty_bytes
                if internal_type == "BinaryField"
                else self.convert_empty_string
            )
        return converters

    def convert_integerfield_value(self, value, expression, connection):
        if value is not None:
            return int(value)
        else:
            return value

    def convert_booleanfield_value(self, value, expression, connection):
        if value in (0, 1):
            value = bool(value)
        return value

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_datefield_value(self, value, expression, connection):
        if isinstance(value, Database.Timestamp):
            value = value.date()
        return value

    def convert_timefield_value(self, value, expression, connection):
        if isinstance(value, Database.Timestamp):
            value = value.time()
        return value

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value

    @staticmethod
    def convert_empty_string(value, expression, connection):
        return "" if value is None else value

    @staticmethod
    def convert_empty_bytes(value, expression, connection):
        return b"" if value is None else value

    def deferrable_sql(self):
        return " DEFERRABLE INITIALLY DEFERRED"

    def no_limit_value(self):
        return None

    def limit_offset_sql(self, low_mark, high_mark):
        fetch, offset = self._get_limit_offset_params(low_mark, high_mark)
        return " ".join(
            sql
            for sql in (
                ("OFFSET %d ROWS" % offset) if offset else None,
                ("FETCH FIRST %d ROWS ONLY" % fetch) if fetch else None,
            )
            if sql
        )

    def last_executed_query(self, cursor, sql, params):
        if params:
            if isinstance(params, list):
                params = tuple(params)
            return sql % params
        # Just return sql when there are no parameters.
        else:
            return sql

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ("iexact", "icontains", "istartswith", "iendswith"):
            return "UPPER(%s)"
        if lookup_type != "isnull" and internal_type in (
            "BinaryField",
            "TextField",
        ):
            return "DBMS_LOB.SUBSTR(%s)"
        return "%s"

    def max_in_list_size(self):
        return 1000

    def max_name_length(self):
        return 128

    def pk_default_value(self):
        return "NULL"

    def prep_for_iexact_query(self, x):
        return x

    # pyodbc에서는 clob을 처리하는 별도의 로직을 제공하지 않습니다.
    # def process_clob(self, value):
    #     if value is None:
    #         return ""
    #     return value

    def quote_name(self, name):
        # SQL92 requires delimited (quoted) names to be case-sensitive.  When
        # not quoted, Tibero has case-insensitive behavior for identifiers, but
        # always defaults to uppercase.
        # We simplify things by making Tibero identifiers always uppercase.
        if not name.startswith('"') and not name.endswith('"'):
            name = '"%s"' % truncate_name(name, self.max_name_length())
        # Tibero puts the query text into a (query % args) construct, so % signs
        # in names need to be escaped. The '%%' will be collapsed back to '%' at
        # that stage so we aren't really making the name longer here.
        name = name.replace("%", "%%")
        return name.upper()

    def regex_lookup(self, lookup_type):
        if lookup_type == "regex":
            match_option = "'c'"
        else:
            match_option = "'i'"
        return "REGEXP_LIKE(%%s, %%s, %s)" % match_option

    def return_insert_columns(self, fields):
        if not fields:
            return "", ()
        field_names = []
        params = []
        for field in fields:
            field_names.append(
                "%s.%s"
                % (
                    self.quote_name(field.model._meta.db_table),
                    self.quote_name(field.column),
                )
            )
            params.append(None)
        return "RETURNING %s INTO %s" % (
            ", ".join(field_names),
            ", ".join(["%s"] * len(params)),
        ), tuple(params)

    def __foreign_key_constraints(self, table_name, recursive):
        with self.connection.cursor() as cursor:
            if recursive:
                cursor.execute(
                    """
                    SELECT
                        user_tables.table_name, rcons.constraint_name
                    FROM
                        user_tables
                    JOIN
                        user_constraints cons
                        ON (user_tables.table_name = cons.table_name
                        AND cons.constraint_type = ANY('P', 'U'))
                    LEFT JOIN
                        user_constraints rcons
                        ON (user_tables.table_name = rcons.table_name
                        AND rcons.constraint_type = 'R')
                    START WITH user_tables.table_name = UPPER(%s)
                    CONNECT BY
                        NOCYCLE PRIOR cons.constraint_name = rcons.r_constraint_name
                    GROUP BY
                        user_tables.table_name, rcons.constraint_name
                    HAVING user_tables.table_name != UPPER(%s)
                    ORDER BY MAX(level) DESC
                    """,
                    (table_name, table_name),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        cons.table_name, cons.constraint_name
                    FROM
                        user_constraints cons
                    WHERE
                        cons.constraint_type = 'R'
                        AND cons.table_name = UPPER(%s)
                    """,
                    (table_name,),
                )
            return cursor.fetchall()

    @cached_property
    def _foreign_key_constraints(self):
        # 512 is large enough to fit the ~330 tables (as of this writing) in
        # Django's test suite.
        return lru_cache(maxsize=512)(self.__foreign_key_constraints)

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []

        truncated_tables = {table.upper() for table in tables}
        constraints = set()
        # Tibero's TRUNCATE CASCADE only works with ON DELETE CASCADE foreign
        # keys which Django doesn't define. Emulate the PostgreSQL behavior
        # which truncates all dependent tables by manually retrieving all
        # foreign key constraints and resolving dependencies.
        for table in tables:
            for foreign_table, constraint in self._foreign_key_constraints(
                table, recursive=allow_cascade
            ):
                if allow_cascade:
                    truncated_tables.add(foreign_table)
                constraints.add((foreign_table, constraint))
        sql = (
            [
                "%s %s %s %s %s %s %s %s;"
                % (
                    style.SQL_KEYWORD("ALTER"),
                    style.SQL_KEYWORD("TABLE"),
                    style.SQL_FIELD(self.quote_name(table)),
                    style.SQL_KEYWORD("DISABLE"),
                    style.SQL_KEYWORD("CONSTRAINT"),
                    style.SQL_FIELD(self.quote_name(constraint)),
                    style.SQL_KEYWORD("KEEP"),
                    style.SQL_KEYWORD("INDEX"),
                )
                for table, constraint in constraints
            ]
            + [
                "%s %s %s;"
                % (
                    style.SQL_KEYWORD("TRUNCATE"),
                    style.SQL_KEYWORD("TABLE"),
                    style.SQL_FIELD(self.quote_name(table)),
                )
                for table in truncated_tables
            ]
            + [
                "%s %s %s %s %s %s;"
                % (
                    style.SQL_KEYWORD("ALTER"),
                    style.SQL_KEYWORD("TABLE"),
                    style.SQL_FIELD(self.quote_name(table)),
                    style.SQL_KEYWORD("ENABLE"),
                    style.SQL_KEYWORD("CONSTRAINT"),
                    style.SQL_FIELD(self.quote_name(constraint)),
                )
                for table, constraint in constraints
            ]
        )
        if reset_sequences:
            sequences = [
                sequence
                for sequence in self.connection.introspection.sequence_list()
                if sequence["table"].upper() in truncated_tables
            ]
            # Since we've just deleted all the rows, running our sequence ALTER
            # code will reset the sequence to 0.
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
        return sql

    # base.py에서 auto field에 대해 Identity keyword를 사용하지 않음에 따라 django 5.1.5
    # Oracle backend의 sequence_reset_sql를 그대로 사용할 수 없습니다.
    # 대신 Django 1.11.29의 Oracle backend의 seuqnce_reset_sql()을 참고했습니다.
    def sequence_reset_by_name_sql(self, style, sequences):
        sql = []
        for sequence_info in sequences:
            sequence_name = self._get_sequence_name(sequence_info["table"], sequence_info["column"] or "id")
            table_name = self.quote_name(sequence_info["table"])
            column_name = self.quote_name(sequence_info["column"] or 'id')
            query = self._sequence_reset_sql % {
                'sequence': sequence_name,
                'table': table_name,
                'column': column_name,
            }
            sql.append(query)
        return sql

    # base.py에서 auto field에 대해 Identity keyword를 사용하지 않음에 따라 django 5.1.5
    # Oracle backend의 sequence_reset_sql를 그대로 사용할 수 없습니다.
    # 대신 Django 1.11.29의 Oracle backend의 seuqnce_reset_sql()을 참고했습니다.
    def sequence_reset_sql(self, style, model_list):
        from django.db import models
        output = []
        query = self._sequence_reset_sql
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    table_name = self.quote_name(model._meta.db_table)
                    sequence_name = self._get_sequence_name(model._meta.db_table, f.column)
                    column_name = self.quote_name(f.column)
                    output.append(query % {'sequence': sequence_name,
                                           'table': table_name,
                                           'column': column_name})
                    # Only one AutoField is allowed per model, so don't
                    # continue to loop
                    break
            for f in model._meta.many_to_many:
                if not f.remote_field.through:
                    table_name = self.quote_name(f.m2m_db_table())
                    sequence_name = self._get_sequence_name(f.m2m_db_table(), "id")
                    column_name = self.quote_name('id')
                    output.append(query % {'sequence': sequence_name,
                                           'table': table_name,
                                           'column': column_name})
        return output

    def start_transaction_sql(self):
        return ""

    def tablespace_sql(self, tablespace, inline=False):
        if inline:
            return "USING INDEX TABLESPACE %s" % self.quote_name(tablespace)
        else:
            return "TABLESPACE %s" % self.quote_name(tablespace)

    def adapt_datefield_value(self, value):
        """
        Transform a date value to an object compatible with what is expected
        by the backend driver for date columns.
        The default implementation transforms the date to text, but that is not
        necessary for Tibero.
        """
        return value

    def adapt_datetimefield_value(self, value):
        """
        Transform a datetime value to an object compatible with what is expected
        by the backend driver for datetime columns.

        If naive datetime is passed assumes that is in UTC. Normally Django
        models.DateTimeField makes sure that if USE_TZ is True passed datetime
        is timezone aware.
        """

        if value is None:
            return None

        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = timezone.make_naive(value, self.connection.timezone)
            else:
                raise ValueError(
                    "Tibero backend does not support timezone-aware datetimes when "
                    "USE_TZ is False."
                )

        return value

    def adapt_timefield_value(self, value):
        if value is None:
            return None

        if isinstance(value, str):
            return datetime.datetime.strptime(value, "%H:%M:%S")

        # Tibero doesn't support tz-aware times
        if timezone.is_aware(value):
            raise ValueError("Tibero backend does not support timezone-aware times.")

        return datetime.datetime(
            1900, 1, 1, value.hour, value.minute, value.second, value.microsecond
        )

    def adapt_decimalfield_value(self, value, max_digits=None, decimal_places=None):
        return value

    def combine_expression(self, connector, sub_expressions):
        lhs, rhs = sub_expressions
        if connector == "%%":
            return "MOD(%s)" % ",".join(sub_expressions)
        elif connector == "&":
            return "BITAND(%s)" % ",".join(sub_expressions)
        elif connector == "|":
            return "BITAND(-%(lhs)s-1,%(rhs)s)+%(lhs)s" % {"lhs": lhs, "rhs": rhs}
        elif connector == "<<":
            return "(%(lhs)s * POWER(2, %(rhs)s))" % {"lhs": lhs, "rhs": rhs}
        elif connector == ">>":
            return "FLOOR(%(lhs)s / POWER(2, %(rhs)s))" % {"lhs": lhs, "rhs": rhs}
        elif connector == "^":
            return "POWER(%s)" % ",".join(sub_expressions)
        elif connector == "#":
            raise NotSupportedError("Bitwise XOR is not supported in Tibero.")
        return super().combine_expression(connector, sub_expressions)

    def _get_no_autofield_sequence_name(self, table):
        """
        Manually created sequence name to keep backward compatibility for
        AutoFields that aren't Tibero identity columns.
        """
        name_length = self.max_name_length() - 3
        return "%s_SQ" % truncate_name(strip_quotes(table), name_length).upper()

    # Oracle backend의 bulk_insert_sql() 대신 mssql에서 사용하는 bulk_insert_sql()를 참고했습니다.
    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def subtract_temporals(self, internal_type, lhs, rhs):
        if internal_type == "DateField":
            lhs_sql, lhs_params = lhs
            rhs_sql, rhs_params = rhs
            params = (*lhs_params, *rhs_params)
            return (
                "NUMTODSINTERVAL(TO_NUMBER(%s - %s), 'DAY')" % (lhs_sql, rhs_sql),
                params,
            )
        return super().subtract_temporals(internal_type, lhs, rhs)

    def autoinc_sql(self, table, column):
        # To simulate auto-incrementing primary keys in Tibero, we have to
        # create a sequence and a trigger.
        args = {
            'sq_name': self._get_sequence_name(table, column),
            'tr_name': self._get_trigger_name(table, column),
            'tbl_name': self.quote_name(table),
            'col_name': self.quote_name(column),
        }
        sequence_sql = """
        DECLARE
            i INTEGER;
        BEGIN
            SELECT COUNT(1) INTO i FROM USER_SEQUENCES
                WHERE SEQUENCE_NAME = '%(sq_name)s';
            IF i = 0 THEN
                EXECUTE IMMEDIATE 'CREATE SEQUENCE "%(sq_name)s"';
            END IF;
        END;
        """ % args
        trigger_sql = """
        CREATE OR REPLACE TRIGGER "%(tr_name)s"
        BEFORE INSERT ON %(tbl_name)s
        FOR EACH ROW
        WHEN (new.%(col_name)s IS NULL)
            BEGIN
                SELECT "%(sq_name)s".nextval
                INTO :new.%(col_name)s FROM dual;
            END;
        """ % args
        return (Statement(template="%(sql)s", sql=sequence_sql, references_table=Table(table, self.quote_name)),
                Statement(template="%(sql)s", sql=trigger_sql, references_table=Table(table, self.quote_name)))

    def _get_sequence_name(self, table, column):
        name_length = self.max_name_length() - 3

        name = strip_quotes(table) + '_' + strip_quotes(column)
        name = truncate_name(name, name_length).upper()
        return '%s_SQ' % name

    def _get_trigger_name(self, table, column):
        name_length = self.max_name_length() - 3

        name = strip_quotes(table) + '_' + strip_quotes(column)
        name = truncate_name(name, name_length).upper()
        return '%s_TR' % name

    def bulk_batch_size(self, fields, objs):
        """Tibero restricts the number of parameters in a query."""
        if fields:
            return self.connection.features.max_query_params // len(fields)
        return len(objs)

    def conditional_expression_supported_in_where_clause(self, expression):
        """
        Tibero supports only EXISTS(...) or filters in the WHERE clause, others
        must be compared with True.
        """
        if isinstance(expression, (Exists, Lookup, WhereNode)):
            return True
        if isinstance(expression, ExpressionWrapper) and expression.conditional:
            return self.conditional_expression_supported_in_where_clause(
                expression.expression
            )
        if isinstance(expression, RawSQL) and expression.conditional:
            return True
        return False
