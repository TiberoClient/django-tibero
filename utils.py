import datetime
import decimal

from .base import Database


class BulkInsertMapper:
    BLOB = "TO_BLOB(%s)"
    DATE = "TO_DATE(%s)"
    INTERVAL = "CAST(%s as INTERVAL DAY(9) TO SECOND(6))"
    NCLOB = "TO_NCLOB(%s)"
    NUMBER = "TO_NUMBER(%s)"
    TIMESTAMP = "TO_TIMESTAMP(%s)"

    types = {
        "AutoField": NUMBER,
        "BigAutoField": NUMBER,
        "BigIntegerField": NUMBER,
        "BinaryField": BLOB,
        "BooleanField": NUMBER,
        "DateField": DATE,
        "DateTimeField": TIMESTAMP,
        "DecimalField": NUMBER,
        "DurationField": INTERVAL,
        "FloatField": NUMBER,
        "IntegerField": NUMBER,
        "PositiveBigIntegerField": NUMBER,
        "PositiveIntegerField": NUMBER,
        "PositiveSmallIntegerField": NUMBER,
        "SmallAutoField": NUMBER,
        "SmallIntegerField": NUMBER,
        "TextField": NCLOB,
        "TimeField": TIMESTAMP,
    }


def encode_connection_string(fields):
    """Encode dictionary of keys and values as an ODBC connection String.

    See [MS-ODBCSTR] document:
    https://msdn.microsoft.com/en-us/library/ee208909%28v=sql.105%29.aspx
    """
    # As the keys are all provided by us, don't need to encode them as we know
    # they are ok.
    return ';'.join(
        '%s=%s' % (k, encode_value(v))
        for k, v in fields.items()
    )

def encode_value(v):
    """If the value contains a semicolon, or starts with a left curly brace,
    then enclose it in curly braces and escape all right curly braces.
    """
    if ';' in v or v.strip(' ').startswith('{'):
        return '{%s}' % (v.replace('}', '}}'),)
    return v


def odbc_connection_string_from_settings(conn_params):
    """Generates a pyodbc connection string from the given connection parameters.

    This function constructs an ODBC connection string using values provided
    in the `conn_params` dictionary. It extracts parameters such as driver,
    DSN, server, database, port, user, and password, and then formats them
    into a properly encoded connection string.
    """
    options = conn_params.get('OPTIONS', {})
    cstr_parts = {
        'DRIVER': options.get('driver', None),
        'DSN': options.get('dsn', None),

        'Server': conn_params.get('HOST', None),
        'Database': conn_params.get('NAME', None),
        'Port': conn_params.get('PORT', None),

        'User': conn_params.get('USER', None),
        'Password': conn_params.get('PASSWORD', None),
    }
    # 값이 None인 항목을 딕셔너리에서 제거 (불필요한 연결 문자열 요소 제거)
    cstr_parts = {k: v for k, v in cstr_parts.items() if v is not None}

    connstr = encode_connection_string(cstr_parts)

    # extra_params are glued on the end of the string without encoding,
    # so it's up to the settings writer to make sure they're appropriate -
    # use encode_connection_string if constructing from external input.
    if options.get('extra_params', None):
        connstr += ';' + options['extra_params']
    return connstr


def dsn(conn_params):
    """Generates a Tibero tbsql connection string from the given connection parameters.

    This function constructs a Tibero connection string in one of the following formats:
    - "host:port/service_name" (Typical for direct connections)
    - "dsn_alias" (Using dsn alias from tbdsn.tbr)

    Raises:
        ValueError: If required parameters are missing.
    """
    host = conn_params.get('HOST', None)
    port = conn_params.get('PORT', None)
    database = conn_params.get('NAME', None)

    options = conn_params.get('OPTIONS', {})
    dsn_alias = options.get('dsn', None)

    if host is not None and port is not None and database is not None:
        return f"{host}:{port}/{database}"
    elif dsn_alias is not None:
        return dsn_alias
    else:
        raise ValueError("'HOST', 'PORT', 'DATABASE' are required or 'dsn' is required")
