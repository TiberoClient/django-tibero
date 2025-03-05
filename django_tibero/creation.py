import sys
import time

from django.conf import settings
from django.db import DatabaseError
from django.db.backends.base.creation import BaseDatabaseCreation
from django.utils.crypto import get_random_string
from django.utils.functional import cached_property

TEST_DATABASE_PREFIX = "test_"


class DatabaseCreation(BaseDatabaseCreation):
    @cached_property
    def _maindb_connection(self):
        """
        This is analogous to other backends' `_nodb_connection` property,
        which allows access to an "administrative" connection which can
        be used to manage the test databases.
        For Tibero, the only connection that can be used for that purpose
        is the main (non-test) connection.
        """
        settings_dict = settings.DATABASES[self.connection.alias]
        user = settings_dict.get("SAVED_USER") or settings_dict["USER"]
        password = settings_dict.get("SAVED_PASSWORD") or settings_dict["PASSWORD"]
        settings_dict = {**settings_dict, "USER": user, "PASSWORD": password}
        DatabaseWrapper = type(self.connection)
        return DatabaseWrapper(settings_dict, alias=self.connection.alias)

    def _create_test_db(self, verbosity=1, autoclobber=False, keepdb=False):
        parameters = self._get_test_db_params()
        with self._maindb_connection.cursor() as cursor:
            if self._test_database_create():
                try:
                    self._execute_test_db_creation(
                        cursor, parameters, verbosity, keepdb
                    )
                except Exception as e:
                    # TBR-7098: Duplicate tablespace가 존재하면 발생하는 에러 번호
                    if "-7098" not in str(e):
                        # All errors except "tablespace already exists" cancel tests
                        self.log("Got an error creating the test database: %s" % e)
                        sys.exit(2)
                    if not autoclobber:
                        confirm = input(
                            "It appears the test database, %s, already exists. "
                            "Type 'yes' to delete it, or 'no' to cancel: "
                            % parameters["user"]
                        )
                    if autoclobber or confirm == "yes":
                        if verbosity >= 1:
                            self.log(
                                "Destroying old test database for alias '%s'..."
                                % self.connection.alias
                            )
                        try:
                            self._execute_test_db_destruction(
                                cursor, parameters, verbosity
                            )
                        except Exception as e:
                            # 테이블 스페이스를 사용하는 유저가 있을 경우 -7355 에러 발생
                            # 따라서 유저를 삭제 후 테이블 스페이스 삭제 재시도
                            if '-7355' in str(e):
                                self._handle_objects_preventing_db_destruction(
                                    cursor, parameters, verbosity, autoclobber
                                )
                            else:
                                # Ran into a database error that isn't about
                                # leftover objects in the tablespace.
                                self.log(
                                    "Got an error destroying the old test database: %s"
                                    % e
                                )
                                sys.exit(2)
                        try:
                            self._execute_test_db_creation(
                                cursor, parameters, verbosity, keepdb
                            )
                        except Exception as e:
                            self.log(
                                "Got an error recreating the test database: %s" % e
                            )
                            sys.exit(2)
                    else:
                        self.log("Tests cancelled.")
                        sys.exit(1)

            if self._test_user_create():
                if verbosity >= 1:
                    self.log("Creating test user...")
                try:
                    self._create_test_user(cursor, parameters, verbosity, keepdb)
                except Exception as e:
                    if "-7100" not in str(e):
                        # All errors except "user already exists" cancel tests
                        self.log("Got an error creating the test user: %s" % e)
                        sys.exit(2)
                    if not autoclobber:
                        confirm = input(
                            "It appears the test user, %s, already exists. Type "
                            "'yes' to delete it, or 'no' to cancel: "
                            % parameters["user"]
                        )
                    if autoclobber or confirm == "yes":
                        try:
                            if verbosity >= 1:
                                self.log("Destroying old test user...")
                            self._destroy_test_user(cursor, parameters, verbosity)
                            if verbosity >= 1:
                                self.log("Creating test user...")
                            self._create_test_user(
                                cursor, parameters, verbosity, keepdb
                            )
                        except Exception as e:
                            self.log("Got an error recreating the test user: %s" % e)
                            sys.exit(2)
                    else:
                        self.log("Tests cancelled.")
                        sys.exit(1)
        # Done with main user -- test user and tablespaces created.
        self._maindb_connection.close()
        self._switch_to_test_user(parameters)
        return self.connection.settings_dict["NAME"]

    def _switch_to_test_user(self, parameters):
        """
        Switch to the user that's used for creating the test database.

        Tibero doesn't have the concept of separate databases under the same
        user, so a separate user is used; see _create_test_db(). The main user
        is also needed for cleanup when testing is completed, so save its
        credentials in the SAVED_USER/SAVED_PASSWORD key in the settings dict.
        """
        real_settings = settings.DATABASES[self.connection.alias]
        real_settings["SAVED_USER"] = self.connection.settings_dict["SAVED_USER"] = (
            self.connection.settings_dict["USER"]
        )
        real_settings["SAVED_PASSWORD"] = self.connection.settings_dict[
            "SAVED_PASSWORD"
        ] = self.connection.settings_dict["PASSWORD"]
        real_test_settings = real_settings["TEST"]
        test_settings = self.connection.settings_dict["TEST"]
        real_test_settings["USER"] = real_settings["USER"] = test_settings["USER"] = (
            self.connection.settings_dict["USER"]
        ) = parameters["user"]
        real_settings["PASSWORD"] = self.connection.settings_dict["PASSWORD"] = (
            parameters["password"]
        )

    def set_as_test_mirror(self, primary_settings_dict):
        """
        Set this database up to be used in testing as a mirror of a primary
        database whose settings are given.
        """
        self.connection.settings_dict["USER"] = primary_settings_dict["USER"]
        self.connection.settings_dict["PASSWORD"] = primary_settings_dict["PASSWORD"]

    def _handle_objects_preventing_db_destruction(
        self, cursor, parameters, verbosity, autoclobber
    ):
        # There are objects in the test tablespace which prevent dropping it
        # The easy fix is to drop the test user -- but are we allowed to do so?
        self.log(
            "There are objects in the old test database which prevent its destruction."
            "\nIf they belong to the test user, deleting the user will allow the test "
            "database to be recreated.\n"
            "Otherwise, you will need to find and remove each of these objects, "
            "or use a different tablespace.\n"
        )
        if self._test_user_create():
            if not autoclobber:
                confirm = input("Type 'yes' to delete user %s: " % parameters["user"])
            if autoclobber or confirm == "yes":
                try:
                    if verbosity >= 1:
                        self.log("Destroying old test user...")
                    self._destroy_test_user(cursor, parameters, verbosity)
                except Exception as e:
                    self.log("Got an error destroying the test user: %s" % e)
                    sys.exit(2)
                try:
                    if verbosity >= 1:
                        self.log(
                            "Destroying old test database for alias '%s'..."
                            % self.connection.alias
                        )
                    self._execute_test_db_destruction(cursor, parameters, verbosity)
                except Exception as e:
                    self.log("Got an error destroying the test database: %s" % e)
                    sys.exit(2)
            else:
                self.log("Tests cancelled -- test database cannot be recreated.")
                sys.exit(1)
        else:
            self.log(
                "Django is configured to use pre-existing test user '%s',"
                " and will not attempt to delete it." % parameters["user"]
            )
            self.log("Tests cancelled -- test database cannot be recreated.")
            sys.exit(1)

    def _destroy_test_db(self, test_database_name, verbosity=1):
        """
        Destroy a test database, prompting the user for confirmation if the
        database already exists. Return the name of the test database created.
        """
        self.connection.settings_dict["USER"] = self.connection.settings_dict[
            "SAVED_USER"
        ]
        self.connection.settings_dict["PASSWORD"] = self.connection.settings_dict[
            "SAVED_PASSWORD"
        ]
        self.connection.close()
        # Hack: 왜인지 모르나 Tibero에서 connection을 닫아도 바로 유저를 삭제할 수 있지가 않습니다.
        #       유저 삭제를 위해 잠시 기다려야 합니다.
        time.sleep(1)
        parameters = self._get_test_db_params()
        with self._maindb_connection.cursor() as cursor:
            if self._test_user_create():
                if verbosity >= 1:
                    self.log("Destroying test user...")
                self._destroy_test_user(cursor, parameters, verbosity)
            if self._test_database_create():
                if verbosity >= 1:
                    self.log("Destroying test database tables...")
                self._execute_test_db_destruction(cursor, parameters, verbosity)
        self._maindb_connection.close()

    def _execute_test_db_creation(self, cursor, parameters, verbosity, keepdb=False):
        if verbosity >= 2:
            self.log("_create_test_db(): dbname = %s" % parameters["user"])
        if self._test_database_tibero_managed_files():
            statements = [
                """
                CREATE TABLESPACE %(tblspace)s
                DATAFILE SIZE %(size)s
                AUTOEXTEND ON NEXT %(extsize)s MAXSIZE %(maxsize)s
                """,
                """
                CREATE TEMPORARY TABLESPACE %(tblspace_temp)s
                TEMPFILE SIZE %(size_tmp)s
                AUTOEXTEND ON NEXT %(extsize_tmp)s MAXSIZE %(maxsize_tmp)s
                """,
            ]
        else:
            statements = [
                """
                CREATE TABLESPACE %(tblspace)s
                DATAFILE '%(datafile)s' SIZE %(size)s REUSE
                AUTOEXTEND ON NEXT %(extsize)s MAXSIZE %(maxsize)s
                """,
                """
                CREATE TEMPORARY TABLESPACE %(tblspace_temp)s
                TEMPFILE '%(datafile_tmp)s' SIZE %(size_tmp)s REUSE
                AUTOEXTEND ON NEXT %(extsize_tmp)s MAXSIZE %(maxsize_tmp)s
                """,
            ]
        # Ignore "tablespace already exists" error when keepdb is on.
        acceptable_tbr_err = "-7098" if keepdb else None
        self._execute_allow_fail_statements(
            cursor, statements, parameters, verbosity, acceptable_tbr_err
        )

    def _create_test_user(self, cursor, parameters, verbosity, keepdb=False):
        if verbosity >= 2:
            self.log("_create_test_user(): username = %s" % parameters["user"])
        statements = [
            """CREATE USER %(user)s
               IDENTIFIED BY "%(password)s"
               DEFAULT TABLESPACE %(tblspace)s
               TEMPORARY TABLESPACE %(tblspace_temp)s
               QUOTA UNLIMITED ON %(tblspace)s
            """,
            # TODO: 일반 유저중에 V$VERSION에 접근권한이 없으면 어떻게 하면 좋을지 고민하기
            """GRANT CREATE SESSION,
                     CREATE TABLE,
                     CREATE SEQUENCE,
                     CREATE PROCEDURE,
                     CREATE TRIGGER
               TO %(user)s""",
            """GRANT SELECT ON V$VERSION
               TO %(user)s""",
        ]
        # Ignore "user already exists" error when keepdb is on
        # TODO: 티베로에 맞게 수정하기
        acceptable_tbr_err = "-7100" if keepdb else None
        success = self._execute_allow_fail_statements(
            cursor, statements, parameters, verbosity, acceptable_tbr_err
        )
        # If the password was randomly generated, change the user accordingly.
        if not success and self._test_settings_get("PASSWORD") is None:
            set_password = 'ALTER USER %(user)s IDENTIFIED BY "%(password)s"'
            self._execute_statements(cursor, [set_password], parameters, verbosity)
        # Most test suites can be run without "create view" and
        # "create materialized view" privileges. But some need it.
        for object_type in ("VIEW", "MATERIALIZED VIEW"):
            extra = "GRANT CREATE %(object_type)s TO %(user)s"
            parameters["object_type"] = object_type
            # TBR-17004: Permission denied.
            success = self._execute_allow_fail_statements(
                cursor, [extra], parameters, verbosity, "-17004"
            )
            if not success and verbosity >= 2:
                self.log(
                    "Failed to grant CREATE %s permission to test user. This may be ok."
                    % object_type
                )

    def _execute_test_db_destruction(self, cursor, parameters, verbosity):
        if verbosity >= 2:
            self.log("_execute_test_db_destruction(): dbname=%s" % parameters["user"])
        statements = [
            "DROP TABLESPACE %(tblspace)s "
            "INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS",
            "DROP TABLESPACE %(tblspace_temp)s "
            "INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS",
        ]
        for statement in statements:
            try:
                self._execute_statements(cursor, [statement], parameters, verbosity)
            except Exception as e:
                # 7073은 Specified tablespace <tablespace name> was not found를 의미합니다.
                if '-7073' in e.args[1]:
                    pass
                else:
                    raise

    def _destroy_test_user(self, cursor, parameters, verbosity):
        if verbosity >= 2:
            self.log("_destroy_test_user(): user=%s" % parameters["user"])
            self.log("Be patient. This can take some time...")
        statements = [
            "DROP USER %(user)s CASCADE",
        ]
        self._execute_statements(cursor, statements, parameters, verbosity)

    def _execute_statements(
        self, cursor, statements, parameters, verbosity, allow_quiet_fail=False
    ):
        for template in statements:
            stmt = template % parameters
            if verbosity >= 2:
                print(stmt)
            try:
                cursor.execute(stmt)
            except Exception as err:
                if (not allow_quiet_fail) or verbosity >= 2:
                    self.log("Failed (%s)" % (err))
                raise

    def _execute_allow_fail_statements(
        self, cursor, statements, parameters, verbosity, acceptable_tbr_err
    ):
        """
        Execute statements which are allowed to fail silently if the Tibero
        error code given by `acceptable_tbr_err` is raised. Return True if the
        statements execute without an exception, or False otherwise.
        """
        try:
            # Statement can fail when acceptable_tbr_err is not None
            allow_quiet_fail = (
                acceptable_tbr_err is not None and len(acceptable_tbr_err) > 0
            )
            self._execute_statements(
                cursor,
                statements,
                parameters,
                verbosity,
                allow_quiet_fail=allow_quiet_fail,
            )
            return True
        except DatabaseError as err:
            description = err.args[1]
            if acceptable_tbr_err is None or acceptable_tbr_err not in description:
                raise
            return False

    def _get_test_db_params(self):
        return {
            "dbname": self._test_database_name(),
            "user": self._test_database_user(),
            "password": self._test_database_passwd(),
            "tblspace": self._test_database_tblspace(),
            "tblspace_temp": self._test_database_tblspace_tmp(),
            "datafile": self._test_database_tblspace_datafile(),
            "datafile_tmp": self._test_database_tblspace_tmp_datafile(),
            "maxsize": self._test_database_tblspace_maxsize(),
            "maxsize_tmp": self._test_database_tblspace_tmp_maxsize(),
            "size": self._test_database_tblspace_size(),
            "size_tmp": self._test_database_tblspace_tmp_size(),
            "extsize": self._test_database_tblspace_extsize(),
            "extsize_tmp": self._test_database_tblspace_tmp_extsize(),
        }

    def _test_settings_get(self, key, default=None, prefixed=None):
        """
        Return a value from the test settings dict, or a given default, or a
        prefixed entry from the main settings dict.
        """
        settings_dict = self.connection.settings_dict
        val = settings_dict["TEST"].get(key, default)
        if val is None and prefixed:
            val = TEST_DATABASE_PREFIX + settings_dict[prefixed]
        return val

    def _test_database_name(self):
        return self._test_settings_get("NAME", prefixed="NAME")

    def _test_database_create(self):
        return self._test_settings_get("CREATE_DB", default=True)

    def _test_user_create(self):
        return self._test_settings_get("CREATE_USER", default=True)

    def _test_database_user(self):
        return self._test_settings_get("USER", prefixed="USER")

    def _test_database_passwd(self):
        password = self._test_settings_get("PASSWORD")
        if password is None and self._test_user_create():
            # Tibero passwords are limited to 30 chars and can't contain symbols.
            password = get_random_string(30)
        return password

    def _test_database_tblspace(self):
        return self._test_settings_get("TBLSPACE", prefixed="USER")

    def _test_database_tblspace_tmp(self):
        settings_dict = self.connection.settings_dict
        return settings_dict["TEST"].get(
            "TBLSPACE_TMP", TEST_DATABASE_PREFIX + settings_dict["USER"] + "_temp"
        )

    def _test_database_tblspace_datafile(self):
        tblspace = "%s.dbf" % self._test_database_tblspace()
        return self._test_settings_get("DATAFILE", default=tblspace)

    def _test_database_tblspace_tmp_datafile(self):
        tblspace = "%s.dbf" % self._test_database_tblspace_tmp()
        return self._test_settings_get("DATAFILE_TMP", default=tblspace)

    def _test_database_tblspace_maxsize(self):
        # 6GB로 설정한 이유는 2GB로 테스트 도중 DATAFILE이 가득 차서 예외가 발생했습니다.
        return self._test_settings_get("DATAFILE_MAXSIZE", default="6000M")

    def _test_database_tblspace_tmp_maxsize(self):
        return self._test_settings_get("DATAFILE_TMP_MAXSIZE", default="200M")

    def _test_database_tblspace_size(self):
        return self._test_settings_get("DATAFILE_SIZE", default="50M")

    def _test_database_tblspace_tmp_size(self):
        return self._test_settings_get("DATAFILE_TMP_SIZE", default="50M")

    def _test_database_tblspace_extsize(self):
        return self._test_settings_get("DATAFILE_EXTSIZE", default="25M")

    def _test_database_tblspace_tmp_extsize(self):
        return self._test_settings_get("DATAFILE_TMP_EXTSIZE", default="25M")

    def _test_database_tibero_managed_files(self):
        return self._test_settings_get("TIBERO_MANAGED_FILES", default=False)

    def _get_test_db_name(self):
        """
        Return the 'production' DB name to get the test DB creation machinery
        to work. This isn't a great deal in this case because DB names as
        handled by Django don't have real counterparts in Tibero.
        """
        return self.connection.settings_dict["NAME"]

    def test_db_signature(self):
        settings_dict = self.connection.settings_dict
        return (
            settings_dict["HOST"],
            settings_dict["PORT"],
            settings_dict["ENGINE"],
            settings_dict["NAME"],
            self._test_database_user(),
        )
