# Tibero backend for Django

## Features

-  Supports Django 5.1
-  Tested on Tibero 6 FS07_CS_2005, Tibero 6.7, Tibero 7.2
-  Passes most of the tests of the Django test suite

## Dependencies

-  pyodbc 5.0 or newer

## Installation

1. Install pyodbc 5.0 (or newer) and Django
2. Install django-tibero:

       pip install django-tibero

## Configuration  

### Example Configurations  

#### Example 1: Basic Connection Using Driver  

This example demonstrates how to configure the connection using the Tibero ODBC driver in Django.
The key aspect is specifying the driver in the `OPTIONS` section, as UnixODBC uses this value to
determine which driver to load. The driver must be defined in the `odbcinst.ini` file.

`settings.py` Configuration

```python
# driver option is necessary because UnixODBC uses the value to choose driver.
# the value must exist in odbcinst.ini.
DATABASES = {
    "ENGINE": "django_tibero",
    "HOST": "127.0.0.1",
    "PORT": 8629,
    "NAME": "tibero",  # SID name
    "USER": "tibero",
    "PASSWORD": "tmax",
    "OPTIONS": {
        "driver": "Tibero7Driver"
    }
}
```

`odbcinst.ini` Configuration

```ini
[Tibero7Driver]
Description=ODBC Driver for Tibero 7
Driver=/home/tibero7/client/lib/libtbodbc.so
```

#### Example 2: Connection Using DSN

This example demonstrates how to configure a connection using a DSN (Data Source Name) defined
in the `odbc.ini` file. The DSN contains the connection details.

`settings.py` Configuration

```python
# dsn name from odbc.ini. In this case, odbc.ini may contain
# user, password. This example overrides DSN user and DSN password if
# user, password exist in odbc.ini file. 
DATABASES = {
    "ENGINE": "django_tibero",
    "USER": "django-user",
    "PASSWORD": "django-password",
    "OPTIONS": {
        "dsn": "Tibero7DSN"  # DSN name from odbc.ini
    }
}
```

`/etc/odbc.ini` Configuration

```ini
[Tibero7DSN]
Description = Tibero7 ODBC Datasource
Driver      = Tibero7Driver
SID         = tibero7
```

### Standard Django settings  

The following keys in the `DATABASES` setting control the behavior of the backend:  

- **ENGINE**
  - String. Must be set to `"django_tibero"`. Required.

- **NAME**
  - String. The SID name of the Tibero database. Optional.  

- **HOST**  
  - String. The database server address. Example: `"127.0.0.1"`, `"localhost"`. Optional.  

- **PORT**  
  - String or Integer. The server instance port. Optional.  

- **USER**
  - String. The database username. Optional.  

- **PASSWORD**
  - String. The database user password. Optional.  

### OPTIONS (Advanced Settings)

In addition to the basic settings, you can configure advanced options using the `OPTIONS`
dictionary in the `DATABASES` setting. These options allow you to control various aspects of the
database connection and query behavior:

- **driver**  
  - String. Specifies the ODBC driver name as defined in `odbcinst.ini`.  
  - driver or dsn option is required because unixodbc uses this option to choose driver.
  - Example: `"Tibero7Driver"`

- **dsn**  
  - String. Specifies the Data Source Name (DSN) as defined in `odbc.ini`.  
  - driver or dsn option is required because unixodbc uses this option to choose driver.
  - Example: `"TiberoDSN"`

- **connection_timeout**  
  - **Type:** Integer  
  - **Description:** Specifies the timeout for the connection attempt, in seconds.  
  - **Default:** `0` (no timeout)  
  - **Behavior:** Passed to the `timeout` parameter of `pyodbc.connect()`.  

- **connection_retries**  
  - **Type:** Integer  
  - **Description:** Defines the number of retry attempts if a connection fails.  
  - **Default:** `5`  
  - **Behavior:** If the initial connection attempt fails, the backend retries up to this number
  of times.  

- **connection_retry_backoff_time**  
  - **Type:** Integer  
  - **Description:** Specifies the wait time (in seconds) between connection retry attempts.  
  - **Default:** `5`  
  - **Behavior:** The system waits this amount of time before retrying a failed connection attempt.  

- **query_timeout**  
  - **Type:** Integer  
  - **Description:** Sets the timeout for SQL queries, in seconds.  
  - **Default:** `0` (no timeout)  
  - **Behavior:**  
    - This timeout applies to all queries executed via the connection.  
    - If a query exceeds this timeout, the database raises an `OperationalError` with SQLSTATE
    `HYT00` or `HYT01`.  
    - Passed to the `timeout` property of the `pyodbc` connection object.  

- **setencoding**  
  - **Type:** String  
  - **Description:** Defines the text encoding for SQL statements and text parameters.  
  - **Behavior:**  
    - Passed to the `setencoding()` method of the `pyodbc` connection.  
    - Must be a valid Python encoding (e.g., `"utf-8"`).  

- **setdecoding**  
  - **Type:** String  
  - **Description:** Defines the text decoding method for reading `SQL_CHAR` or `SQL_WCHAR` values
  from the database.  
  - **Behavior:**  
    - Passed to the `setdecoding()` method of the `pyodbc` connection.  
    - Must be a valid Python encoding (e.g., `"utf-16le"`).  


### Connection Handling Logic inside Tibero ODBC driver 

The following logic explains how options are used when UnixODBC calls
the driver’s SQLDriverConnect() function.

1. Before connection string is passed to pyodbc.connect() method. Following logic inside django-tibero
backend transforms options in `settings.py`. And then, transform to ODBC Connection String.
This logic is not handled in Tibero driver.

```python

cstr_parts = {
    'DRIVER': options.get('driver', None),
    'DSN': options.get('dsn', None),
    'Server': conn_params.get('HOST', None),
    'Database': conn_params.get('NAME', None),
    'Port': str(conn_params.get('PORT', None)),
    'User': conn_params.get('USER', None),
    'Password': conn_params.get('PASSWORD', None),

# Example of connection_string is "Driver=Tibero7Driver;Server=127.0.0.1;Port=8629;Database=tibero7;User=django;Password=password"
connection_string = odbc_connection_string_from_settings(cstr_parts)

connection = pyodbc.connect(connection_string)
```

2. Connection Logic inside Tibero Driver

```
REQUIRED_KEYWORDS = ['Server', 'IpAddress', 'Port', 'Database', 'User', 'Password']
    
IF Server keyword is in connection string:
    IF NOT all(keyword in connection string for keyword in REQUIRED_KEYWORDS):
        RAISE ConnectionError("Missing required connection parameters")
    ELSE:
        Use REQUIRED_KEYWORDS in the connection string to connect


ELSE IF 'DSN' keyword is in connection_string:
    # find odbc.ini file
    IF environment_variable_exists('ODBCINI'):
        config_file = $ODBCINI
    ELSE IF file_exists('$HOME/.odbc.ini'):
        config_file = '$HOME/.odbc.ini'
    ELSE:
        config_file = '/etc/odbc.ini'

    IF 'Server' or 'IpAddress' exists in config_file:
        REQUIRE 'Port' and ('Database' or 'DB') in config_file
        
        IF 'User' or 'Password' in connection_string:
            Override credentials, otherwise use 'User' and 'Password key/value from config_file 
     
        Connect to Tibero Database 
    ELSE:
        USE credentials from tbdsn.tbr
        
        IF 'User' or 'Password' in connection_string:
            Override credentials, otherwise use 'User' and 'Password key/value from config_file
        Connect to Tibero Database
```
