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