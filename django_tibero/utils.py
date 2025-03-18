import datetime
import re


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
        'Port': str(conn_params.get('PORT', None)),

        'User': conn_params.get('USER', None),
        'Password': conn_params.get('PASSWORD', None),
    }
    # 값이 None인 항목을 딕셔너리에서 제거 (불필요한 연결 문자열 요소 제거)
    cstr_parts = {k: v for k, v in cstr_parts.items() if v is not None or v == ""}

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
    port = str(conn_params.get('PORT', None))
    database = conn_params.get('NAME', None)

    options = conn_params.get('OPTIONS', {})
    dsn_alias = options.get('dsn', None)

    if host is not None and port is not None and database is not None:
        return f"{host}:{port}/{database}"
    elif dsn_alias is not None:
        return dsn_alias
    else:
        raise ValueError("'HOST', 'PORT', 'DATABASE' are required or 'dsn' is required")


def timedelta_to_tibero_interval_string(timedelta):
    # Python의 timedelta 객체를 Tibero의 INTERVAL DAY(9) TO SECOND(6) 문자열로 변환하는 함수입니다.
    #
    # timedelta가 양수인 경우, 아래의 코드처럼 days와 seconds 속성을 단순히 추출해 시, 분, 초로
    # 계산해 문자열을 만들 수 있습니다.
    #
    #     days = timedelta.days
    #     seconds = timedelta.seconds
    #
    #     h = seconds // 3600
    #     m = (seconds % 3600) // 60
    #     s = seconds % 60
    #     ms = timedelta.microseconds
    #     return f"INTERVAL '{days} {h}:{m}:{s}.{ms:06}' DAY(9) TO SECOND(6)"
    #
    # 그러나 timedelta가 음수일 경우 Python에서 이를 표현하는 방식 때문에 주의가 필요합니다.
    #
    # 예를 들어, `datetime.timedelta(seconds=-3)`을 문자열로 변환하면:
    #
    #     str(datetime.timedelta(seconds=-3))-> '-1 day, 23:59:57'
    #
    # 즉, '-1 day + 23:59:57 = -3초'와 같은 방식입니다.
    # 이처럼 복잡한 방식으로 음수를 표현하기 때문에 단순히 days, seconds 필드만 참조해서 문자열을 만들면
    # 정확하지 않습니다.
    #
    # 따라서 timedelta의 전체 마이크로초 값을 절댓값으로 가져온 후,
    # 각 단위 (days, hours, minutes, seconds, microseconds)로 재계산하여 부호와 함께 포맷팅합니다.
    # 자세한 설명은 https://docs.python.org/3/library/datetime.html#datetime.timedelta.total_seconds
    # 을 참고하세요.

    total_microseconds = abs(timedelta // datetime.timedelta(microseconds=1))
    if timedelta.days >= 0:
        sign = '+'
    else:
        sign = '-'

    d, remainder = divmod(total_microseconds, 86400 * 1_000_000)
    h, remainder = divmod(remainder, 3600 * 1_000_000)
    m, remainder = divmod(remainder, 60 * 1_000_000)
    s, ms = divmod(remainder, 1_000_000)

    # 마이크로초를 소수점 이하 6자리로 변환
    return f"INTERVAL '{sign}{d} {h}:{m}:{s}.{ms:06}' DAY(9) TO SECOND(6)"


paren_number_pattern = re.compile(r'\(\d+\)')


def remove_parentheses_numbers(data_type):
    """
    SQL 타입명에서 (n)같이 parenthesis와 그 안의 숫자를 제거합니다.
    예를 들어, 다음과 같은 문자열이 아래와 같이 변환됩니다.

    변환 전:
    "TIMESTAMP(1)",
    "TIMESTAMP(2)",
    "INTERVAL DAYS(3) SECONDS(4)",
    "INTERVAL DAYS(9) SECONDS(10)"

    변환 후:
    "TIMESTAMP",
    "TIMESTAMP",
    "INTERVAL DAYS SECONDS",
    "INTERVAL DAYS SECONDS"
    """
    return paren_number_pattern.sub('', data_type)