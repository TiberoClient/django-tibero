"""
Tibero database backend for Django.

Requires pyodbc: https://github.com/mkleehammer/pyodbc
"""

import datetime
import time
import os
import platform
from contextlib import contextmanager

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import debug_transaction
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property
from django.utils.version import get_version_tuple

try:
    import pyodbc as Database
except ImportError as e:
    raise ImproperlyConfigured("Error loading pyodbc module: %s" % e)

pyodbc_ver = get_version_tuple(Database.version)
if pyodbc_ver < (5, 0):
    raise ImproperlyConfigured("pyodbc 5.0 or newer is required; you have %s" % Database.version)

if hasattr(settings, 'DATABASE_CONNECTION_POOLING'):
    if not settings.DATABASE_CONNECTION_POOLING:
        Database.pooling = False

def _setup_environment(environ):
    # Cygwin requires some special voodoo to set the environment variables
    # properly so that Tibero will see them.
    if platform.system().upper().startswith("CYGWIN"):
        try:
            import ctypes
        except ImportError as e:
            raise ImproperlyConfigured(
                "Error loading ctypes: %s; "
                "the Tibero backend requires ctypes to "
                "operate correctly under Cygwin." % e
            )
        kernel32 = ctypes.CDLL("kernel32")
        for name, value in environ:
            kernel32.SetEnvironmentVariableA(name, value)
    else:
        os.environ.update(environ)

# TODO: 환경 변수뿐만 아니라 settings.py를 통해 값을 받는 방식도 고려하기
_setup_environment(
    [
        # pyodbc 5.0 이상을 사용할려면 libtbodbc의 환경변수인 TBCLI_WCHAR_TYPE을 UCS2로 설정
        # 해야 합니다.
        ('TBCLI_WCHAR_TYPE', 'UCS2'),

        # returning into clause 지원을 위해 필요한 환경변수
        # TODO: 현재 django에서 지원하는 oracle backend의 설정인 use_returning_into는 문제가 있습니다.
        #       django 개발자와 대화한 결과 이 설정은 deprecated된 가능성이 높습니다. 그에 맞게 티베로에서도
        #       use_returning_into 설정 및 관련 코드를 삭제해야 합니다.
        #       refs: https://code.djangoproject.com/ticket/36189
        ('TBCLI_COMPAT_ALCHEMY', 'YES'),
    ]
)

from .client import DatabaseClient # noqa
from .creation import DatabaseCreation  # noqa
from .features import DatabaseFeatures  # noqa
from .introspection import DatabaseIntrospection  # noqa
from .operations import DatabaseOperations  # noqa
from .schema import DatabaseSchemaEditor  # noqa
from .utils import odbc_connection_string_from_settings, timedelta_to_tibero_interval_string  # noqa
from .validation import DatabaseValidation  # noqa

@contextmanager
def wrap_tibero_errors():
    try:
        yield
    except Database.Error as e:
        msg = str(e)
        # pyodbc raises a pyodbc.Error exception with the
        # following attributes and values:
        #  type of args is tuple
        #  args[0] = ODBC error code
        #  args[1] = 'TBR-10008': parent key not found
        #            or:
        #            'TBR-10007: UNIQUE constraint violation
        # Convert that case to Django's IntegrityError exception.
        if "-10008" in msg or "-10007" in msg:
            raise IntegrityError(*e.args)
        else:
            raise

def handle_interval_day_to_second(dto: bytes):
    interval_str = dto.decode()
    days, time_str = interval_str.split()

    days = int(days)
    hours, minutes, rest = time_str.split(":")
    hours = int(hours)
    minutes = int(minutes)

    if "." in rest:
        seconds, microseconds = map(int, rest.split("."))
    else:
        seconds = int(rest)
        microseconds = 0

    # timedelta 객체 생성
    return datetime.timedelta(
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=microseconds,
    )


# TODO: Tibero 6와 7 모두 같은 operators 사용가능하다면 이 클래스는 필요없어지게 됩니다.
#       필요없는 것이 확인되면 삭제하기
class _UninitializedOperatorsDescriptor:
    def __get__(self, instance, cls=None):
        # If connection.operators is looked up before a connection has been
        # created, transparently initialize connection.operators to avert an
        # AttributeError.
        if instance is None:
            raise AttributeError("operators not available as class attribute")
        # Creating a cursor will initialize the operators.
        instance.cursor().close()
        return instance.__dict__["operators"]


class DatabaseWrapper(BaseDatabaseWrapper):
    # vendor 이름은 oracle로 나둬야 django code base안에 있는 oracle compiler를
    # 사용할 수 있습니다.
    vendor = "oracle"
    display_name = "Tibero"

    # This dictionary maps Field objects to their associated Tibero column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    #
    # Any format strings starting with "qn_" are quoted before being used in the
    # output (the "qn_" prefix is stripped before the lookup is performed.
    #
    # AutoField의 값에 " " (공백)가 없으면 IntegerField에서 AutoField로 변경할 때 실제 필드 변경이
    # 발생하지 않는 문제를 해결하기 위함입니다. `schema.py`의 `alter_field()` 함수에서, old field와
    # new field의 타입이 다르면 타입 변경 쿼리를 생성하는데, 문제는 Tibero의 AutoField와 IntegerField가
    # 동일한 `NUMBER(11)` 타입을 사용하기 때문에, 실제로 타입 변경 및 필요한 쿼리 실행이 되지 않습니다. 이로
    # 인해 새로운 AutoField 타입에서 sequence가 생성되어야 할 경우에도 생성되지 않는 문제가 발생합니다.
    # 따라서 우회 방법으로 AutoField 타입에 공백을 추가하여 string 비교에서 불일치를 유도했습니다.
    # 이 문제를 확인할 수 있는 테스트는
    # `schema.tests.SchemaTests.test_alter_int_pk_to_bigautofield_pk`입니다.
    data_types = {
        "AutoField": "NUMBER(11)  ",  # suffix로 space를 넣은 것은 실수가 아닙니다.
        "BigAutoField": "NUMBER(19)  ", # suffix로 space를 넣은 것은 실수가 아닙니다.
        "BinaryField": "BLOB",
        "BooleanField": "NUMBER(1)",
        "CharField": "NVARCHAR2(%(max_length)s)",
        "DateField": "DATE",
        "DateTimeField": "TIMESTAMP",
        "DecimalField": "NUMBER(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "INTERVAL DAY(9) TO SECOND(6)",
        "FileField": "NVARCHAR2(%(max_length)s)",
        "FilePathField": "NVARCHAR2(%(max_length)s)",
        "FloatField": "DOUBLE PRECISION",
        "IntegerField": "NUMBER(11)",
        # "JSONField": "NCLOB", # TODO: django에서 tibero json을 사용할 수 있는 날이 오면 지원하기
        "BigIntegerField": "NUMBER(19)",
        "IPAddressField": "VARCHAR2(15)",
        "GenericIPAddressField": "VARCHAR2(39)",
        "OneToOneField": "NUMBER(11)",
        "PositiveBigIntegerField": "NUMBER(19)",
        "PositiveIntegerField": "NUMBER(11)",
        "PositiveSmallIntegerField": "NUMBER(11)",
        "SlugField": "NVARCHAR2(%(max_length)s)",
        "SmallAutoField": "NUMBER(5)  ", # suffix로 space를 넣은 것은 실수가 아닙니다.
        "SmallIntegerField": "NUMBER(11)",
        "TextField": "NCLOB",
        "TimeField": "TIMESTAMP",
        "URLField": "VARCHAR2(%(max_length)s)",
        "UUIDField": "VARCHAR2(32)",
    }
    data_type_check_constraints = {
        "BooleanField": "%(qn_column)s IN (0,1)",
        # TODO: CHECK(... IS JSON) 형식을 6와 7에서 지원을 하지 않습니다. 나중에 지원되면 추가하기
        # "JSONField": "%(qn_column)s IS JSON",
        "PositiveBigIntegerField": "%(qn_column)s >= 0",
        "PositiveIntegerField": "%(qn_column)s >= 0",
        "PositiveSmallIntegerField": "%(qn_column)s >= 0",
    }

    # TODO: 티베로에서도 안되는지 확인하기
    # Tibero doesn't support a database index on these columns.
    _limited_data_types = ("clob", "nclob", "blob")

    operators = _UninitializedOperatorsDescriptor()

    # TODO: Tibero 6와 Tibero 7 모두 지원되는 operator를 가지고 있는지 확인하기
    #       만약 6와 7 모두 지원안되는 operator를 가지고 있다면 _standard_operators를 수정하거나
    #       _likec_operators 처럼 과거 버전에 호환되는 dictionary를 만들기
    #       호환을 위해 사용되는 _likec_operators의 예시는 init_connection_state() 메서드를 참고하기
    _standard_operators = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": (
            "LIKE TRANSLATE(%s USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)"
        ),
        "icontains": (
            "LIKE UPPER(TRANSLATE(%s USING NCHAR_CS)) "
            "ESCAPE TRANSLATE('\\' USING NCHAR_CS)"
        ),
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": (
            "LIKE TRANSLATE(%s USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)"
        ),
        "endswith": (
            "LIKE TRANSLATE(%s USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)"
        ),
        "istartswith": (
            "LIKE UPPER(TRANSLATE(%s USING NCHAR_CS)) "
            "ESCAPE TRANSLATE('\\' USING NCHAR_CS)"
        ),
        "iendswith": (
            "LIKE UPPER(TRANSLATE(%s USING NCHAR_CS)) "
            "ESCAPE TRANSLATE('\\' USING NCHAR_CS)"
        ),
    }

    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, %, _)
    # should be escaped on the database side.
    #
    # Note: we use str.format() here for readability as '%' is used as a wildcard for
    # the LIKE operator.
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    _pattern_ops = {
        "contains": "'%%' || {} || '%%'",
        "icontains": "'%%' || UPPER({}) || '%%'",
        "startswith": "{} || '%%'",
        "istartswith": "UPPER({}) || '%%'",
        "endswith": "'%%' || {}",
        "iendswith": "'%%' || UPPER({})",
    }

    _standard_pattern_ops = {
        k: "LIKE TRANSLATE( " + v + " USING NCHAR_CS)"
        " ESCAPE TRANSLATE('\\' USING NCHAR_CS)"
        for k, v in _pattern_ops.items()
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    validation_class = DatabaseValidation

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        use_returning_into = self.settings_dict["OPTIONS"].get(
            "use_returning_into", True
        )
        self.features.can_return_columns_from_insert = use_returning_into

    def get_database_version(self):
        return self.tibero_version

    # TODO: setting에 불가능한 조합이 있다면 예외 발생시키기 (mssql, oracle, postgresql 참고하기)
    def get_connection_params(self):
        return self.settings_dict.copy()

    @async_unsafe
    def get_new_connection(self, conn_params):
        connstr = odbc_connection_string_from_settings(conn_params)

        options = conn_params.get('OPTIONS', {})
        timeout = options.get('connection_timeout', 0)
        retries = options.get('connection_retries', 5)
        backoff_time = options.get('connection_retry_backoff_time', 5)
        query_timeout = options.get('query_timeout', 0)
        setencoding = options.get('setencoding', None)
        setdecoding = options.get('setdecoding', None)

        conn = None
        retry_count = 0
        while conn is None:
            try:
                conn = Database.connect(connstr, timeout=timeout)
            except Exception as e:
                # TODO: mssql의 경우 mssql server에서 pyodbc로 전달되는 error code에 따라
                #       retry를 하는 경우와 안하는 경우로 나뉘어집니다. 티베로의 경우 현재 개발자 (전영배)가
                #       제공되는 에러 코드가 완전히 파악이 되지 않은 상태여서 모든 예외 상황에 retry를 하도록
                #       했습니다.
                if retry_count < retries:
                    time.sleep(backoff_time)
                    retry_count = retry_count + 1
                else:
                    raise

        # Handling values from 'INTERVAL DAY TO SECOND' columns
        # source: https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
        conn.add_output_converter(
            Database.SQL_INTERVAL_DAY_TO_SECOND,
            handle_interval_day_to_second
        )

        conn.timeout = query_timeout
        if setencoding:
            for entry in setencoding:
                conn.setencoding(**entry)
        if setdecoding:
            for entry in setdecoding:
                conn.setdecoding(**entry)
        return conn

    def init_connection_state(self):
        super().init_connection_state()
        cursor = self.create_cursor()
        # Set the territory first. The territory overrides NLS_DATE_FORMAT
        # and NLS_TIMESTAMP_FORMAT to the territory default. When all of
        # these are set in single statement it isn't clear what is supposed
        # to happen.
        cursor.execute("ALTER SESSION SET NLS_TERRITORY = 'AMERICA'")
        # Set Tibero date to ANSI date format.  This only needs to execute
        # once when we create a new connection. We also set the Territory
        # to 'AMERICA' which forces Sunday to evaluate to a '1' in
        # TO_CHAR().
        cursor.execute(
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + (" TIME_ZONE = 'UTC'" if settings.USE_TZ else "")
        )
        cursor.close()

        if "operators" not in self.__dict__:
                self.operators = self._standard_operators
                self.pattern_ops = self._standard_pattern_ops

        # Ensure all changes are preserved even when AUTOCOMMIT is False.
        if not self.get_autocommit():
            self.commit()

    @async_unsafe
    def create_cursor(self, name=None):
        return CursorWrapper(self.connection.cursor(), self)

    def _commit(self):
        if self.connection is not None:
            with debug_transaction(self, "COMMIT"), wrap_tibero_errors():
                return self.connection.commit()

    # Tibero doesn't support releasing savepoints. But we fake them when query
    # logging is enabled to keep query counts consistent with other backends.
    def _savepoint_commit(self, sid):
        if self.queries_logged:
            self.queries_log.append(
                {
                    "sql": "-- RELEASE SAVEPOINT %s (faked)" % self.ops.quote_name(sid),
                    "time": "0.000",
                }
            )

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    # TODO: 아래 코드는 Tibero에서 작동안하는 sql statements입니다.
    #       어떻게 해결해야할지 고민하기
    #       어쩌면 mysql의 check_constraints() 방법을 사용할 수도 있을 것 같습니다.
    #       django test suite를 통과할려면 아래의 sql 지원이 필요합니다. 반드시 필요한 것은
    #       아니나 있으면 좋을 것 같습니다.
    # def check_constraints(self, table_names=None):
    #     """
    #     Check constraints by setting them to immediate. Return them to deferred
    #     afterward.
    #     """
    #     with self.cursor() as cursor:
    #         cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    #         cursor.execute("SET CONSTRAINTS ALL DEFERRED")

    def is_usable(self):
        try:
            self.create_cursor().execute("SELECT 1" + self.features.bare_select_suffix)
        except Database.Error:
            return False
        else:
            return True

    @cached_property
    def tibero_version(self):
        with self.temporary_connection() as cursor:
            cursor.execute("SELECT * FROM V$VERSION")
            version_dict = {key: value for key, value, _ in cursor}

            # TODO: Tibero 7의 PRODUCT_MINOR가 ' '처럼 space 하나인 경우가 있는데
            #       왜 그런지 모르겠습니다. 티베로 버그인지 찾아보기
            #       아래의 ".strip() or 0"은 이 이상한 현상으로 인해 생기는 문제를 해결하는
            #       임시 코드입니다.
            product_major = version_dict.get('PRODUCT_MAJOR')
            product_minor = version_dict.get('PRODUCT_MINOR').strip() or 0
            return int(product_major), int(product_minor)

    # TODO: pyodbc 버전만 지원할 예정이기 때문에 필요없는 method일 수도 있습니다.
    #       이 메서드를 사용하는 features.py를 참고해서 지울지 결정하기
    @cached_property
    def pyodbc_version(self):
        # 처음엔 libtbodbc의 버전을 반환하는게 맞다고 생각했습니다. 그런데 생각해보니 libtbodbc의
        # 버전이 다르게 해도 pyodbc의 행동을 변경할 수 있는 것이 아닙니다. 오히려 pyodbc 버전을
        # 다르게 해야 기본 설정이 달라지기 때문에 pyodbc 버전 반환을 하게 되었습니다.
        return Database.version


class CursorWrapper:
    """
    A wrapper around the pyodbc's cursor that takes in account a) some pyodbc
    DB-API 2.0 implementation and b) some common ODBC driver particularities.
    """

    def __init__(self, cursor, connection):
        self.active = True
        self.cursor = cursor
        self.connection = connection

    def close(self):
        if self.active:
            self.active = False
            self.cursor.close()

    def _format_sql(self, sql, params):
        # pyodbc uses '?' instead of '%s' as parameter placeholder.
        if params != () and params != []:
            sql = sql % tuple('?' * len(params))
        return sql

    def _fix_params(self, params):
        """
        pyodbc와 Django과 함께 잘 작동을 할려면 cursor.execute()하기 전에 파라미터의 타입과 값을
        변경해야 하는 일이 있습니다. 그런데 SQLAlchemy와 다르게 cursor.execute() 이전에 값을
        변경하는 방법을 제공하지 않는 것 같습니다. 따라서 이 메서드를 따로 구현했습니다. 다만
        SQLAlchemy와 다르게 Django에서는 모든 파라미터를 iterating 하기때문에 성능상 저하기 될 수
        있습니다.
        """
        new_params = []
        for param in params:
            # pyodbc는 timedelta를 지원하지 않습니다. Django의 DurationField같은 타입을 지원하기
            # 위해서는 timedelta 타입을 문자열로 변경해야 합니다.
            # 하지만 여전히 prepared statement에서 사용 못하는 문제가 있으며 이로 인해
            # timedelta 관련 연산에서는 문제가 발생할 수 있습니다.
            if isinstance(param, datetime.timedelta):
                new_params.append(timedelta_to_tibero_interval_string(param))
            else:
                new_params.append(param)

        return new_params

    def execute(self, sql, params=()):
        sql = self._format_sql(sql, params)
        params = self._fix_params(params)
        with wrap_tibero_errors():
            return self.cursor.execute(sql, params)

    def executemany(self, sql, params_list=()):
        if not params_list:
            return None
        # 유저가 sequence가 아닌 generator을 params_list에 넘겨주는 경우가 있습니다.
        raw_params_list = [p for p in params_list]
        sql = self._format_sql(sql, raw_params_list[0])
        fixed_params_list = self._fix_params(raw_params_list)
        with wrap_tibero_errors():
            return self.cursor.executemany(sql, fixed_params_list)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            row = tuple(row)
        return row

    def fetchmany(self, chunk):
        return list(map(tuple, self.cursor.fetchmany(chunk)))

    def fetchall(self):
        return list(map(tuple, self.cursor.fetchall()))

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)
