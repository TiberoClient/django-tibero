import copy
import datetime
import re

from django.db import Error
from django.db.backends.base.schema import (
    BaseDatabaseSchemaEditor,
    _related_non_m2m_objects,
)

from .utils import timedelta_to_tibero_interval_string


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_alter_column_type = "MODIFY %(column)s %(type)s%(collation)s"
    sql_alter_column_null = "MODIFY %(column)s NULL"
    sql_alter_column_not_null = "MODIFY %(column)s NOT NULL"
    sql_alter_column_default = "MODIFY %(column)s DEFAULT %(default)s"
    sql_alter_column_no_default = "MODIFY %(column)s DEFAULT NULL"
    sql_alter_column_no_default_null = sql_alter_column_no_default

    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"
    sql_create_column_inline_fk = (
        "CONSTRAINT %(name)s REFERENCES %(to_table)s(%(to_column)s)%(deferrable)s"
    )
    sql_delete_table = "DROP TABLE %(table)s CASCADE CONSTRAINTS"
    sql_create_index = "CREATE INDEX %(name)s ON %(table)s (%(columns)s)%(extra)s"

    def quote_value(self, value):
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return "'%s'" % value
        elif isinstance(value, datetime.timedelta):
            return timedelta_to_tibero_interval_string(value)
        elif isinstance(value, str):
            return "'%s'" % value.replace("'", "''")
        elif isinstance(value, (bytes, bytearray, memoryview)):
            return "'%s'" % value.hex()
        elif isinstance(value, bool):
            return "1" if value else "0"
        else:
            return str(value)

    def add_field(self, model, field):
        super().add_field(model, field)
        self._add_sequence_to_deferred_sql_list_if_autofield(model, field)

    def remove_field(self, model, field):
        # If the column is an identity column, drop the identity before
        # removing the field.
        if self._is_identity_column(model._meta.db_table, field.column):
            self._drop_identity(model._meta.db_table, field.column)
        super().remove_field(model, field)

    def delete_model(self, model):
        # Run superclass action
        super().delete_model(model)
        # Clean up manually created sequence.
        self.execute(
            """
            DECLARE
                i INTEGER;
            BEGIN
                SELECT COUNT(1) INTO i FROM USER_SEQUENCES
                    WHERE SEQUENCE_NAME = '%(sq_name)s';
                IF i = 1 THEN
                    EXECUTE IMMEDIATE 'DROP SEQUENCE "%(sq_name)s"';
                END IF;
            END;
        """
            % {
                "sq_name": self.connection.ops._get_no_autofield_sequence_name(
                    model._meta.db_table
                )
            }
        )

    # TODO: error code를 티베로에 맞게 고치기
    def alter_field(self, model, old_field, new_field, strict=False):
        try:
            super().alter_field(model, old_field, new_field, strict)
        except Error as e:
            description = str(e)
            # If we're changing type to an unsupported type we need a
            # SQLite-ish workaround
            if "-7237" in description:
                self._alter_field_type_workaround(model, old_field, new_field)
            # If an identity column is changing to a non-numeric type, drop the
            # identity first.
            elif "-7535" in description:
                self._drop_identity(model._meta.db_table, old_field.column)
                self.alter_field(model, old_field, new_field, strict)
            # If a primary key column is changing to an identity column, drop
            # the primary key first.
            # 현재 Tibero backend 구현에서는 아래 identity column관련된 에러가 발생할 수 없습니다.
            # 다만 나중에 deferred_sql에서 sequence를 사용하는대신 identity column을 사용하는 미래
            # 를 생각해 원본 코드를 tibero에 수정만 하고 남겼습니다.
            elif "-7548" in description and old_field.primary_key:
                self._delete_primary_key(model, strict=True)
                self._alter_field_type_workaround(model, old_field, new_field)
            else:
                raise

    def _alter_field_type_workaround(self, model, old_field, new_field):
        """
        Tibero refuses to change from some type to other type.
        What we need to do instead is:
        - Add a nullable version of the desired field with a temporary name. If
          the new column is an auto field, then the temporary column can't be
          nullable.
        - Update the table to transfer values from old to new
        - Drop old column
        - Rename the new column and possibly drop the nullable property
        """
        # Make a new field that's like the new one but with a temporary
        # column name.
        new_temp_field = copy.deepcopy(new_field)
        new_temp_field.null = new_field.get_internal_type() not in (
            "AutoField",
            "BigAutoField",
            "SmallAutoField",
        )
        new_temp_field.column = self._generate_temp_name(new_field.column)
        # Add it
        self.add_field(model, new_temp_field)
        # Explicit data type conversion
        # https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf
        # /Data-Type-Comparison-Rules.html#GUID-D0C5A47E-6F93-4C2D-9E49-4F2B86B359DD
        new_value = self.quote_name(old_field.column)
        old_type = old_field.db_type(self.connection)
        if re.match("^N?CLOB", old_type):
            new_value = "TO_CHAR(%s)" % new_value
            old_type = "VARCHAR2"
        if re.match("^N?VARCHAR2", old_type):
            new_internal_type = new_field.get_internal_type()
            if new_internal_type == "DateField":
                new_value = "TO_DATE(%s, 'YYYY-MM-DD')" % new_value
            elif new_internal_type == "DateTimeField":
                new_value = "TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS.FF')" % new_value
            elif new_internal_type == "TimeField":
                # TimeField are stored as TIMESTAMP with a 1900-01-01 date part.
                new_value = "CONCAT('1900-01-01 ', %s)" % new_value
                new_value = "TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS.FF')" % new_value
        # Transfer values across
        self.execute(
            "UPDATE %s set %s=%s"
            % (
                self.quote_name(model._meta.db_table),
                self.quote_name(new_temp_field.column),
                new_value,
            )
        )
        # Drop the old field
        self.remove_field(model, old_field)
        # Rename and possibly make the new field NOT NULL
        super().alter_field(model, new_temp_field, new_field)
        # Recreate foreign key (if necessary) because the old field is not
        # passed to the alter_field() and data types of new_temp_field and
        # new_field always match.
        new_type = new_field.db_type(self.connection)
        if (
            (old_field.primary_key and new_field.primary_key)
            or (old_field.unique and new_field.unique)
        ) and old_type != new_type:
            for _, rel in _related_non_m2m_objects(new_temp_field, new_field):
                if rel.field.db_constraint:
                    self.execute(
                        self._create_fk_sql(rel.related_model, rel.field, "_fk")
                    )

    def _alter_column_type_sql(
        self, model, old_field, new_field, new_type, old_collation, new_collation
    ):
        auto_field_types = {"AutoField", "BigAutoField", "SmallAutoField"}
        # Drop the identity if migrating away from AutoField.
        if (
            old_field.get_internal_type() in auto_field_types
            and new_field.get_internal_type() not in auto_field_types
            and self._is_identity_column(model._meta.db_table, new_field.column)
        ):
            self._drop_identity(model._meta.db_table, new_field.column)

        # TODO: GENERATED [BY DEFAULT ON NULL | AS AWLAYS]가 지원되면 삭제해야하는 코드입니다.
        self._add_sequence_to_deferred_sql_list_if_autofield(model, new_field)

        return super()._alter_column_type_sql(
            model, old_field, new_field, new_type, old_collation, new_collation
        )

    def normalize_name(self, name):
        """
        Get the properly shortened and uppercased identifier as returned by
        quote_name() but without the quotes.
        """
        nn = self.quote_name(name)
        if nn[0] == '"' and nn[-1] == '"':
            nn = nn[1:-1]
        return nn

    def _generate_temp_name(self, for_name):
        """Generate temporary names for workarounds that need temp columns."""
        suffix = hex(hash(for_name)).upper()[1:]
        return self.normalize_name(for_name + "_" + suffix)

    def prepare_default(self, value):
        return self.quote_value(value)

    def _field_should_be_indexed(self, model, field):
        create_index = super()._field_should_be_indexed(model, field)
        db_type = field.db_type(self.connection)
        if (
            db_type is not None
            and db_type.lower() in self.connection._limited_data_types
        ):
            return False
        return create_index

    def _is_identity_column(self, table_name, column_name):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM user_sequences
                WHERE user_sequences.sequence_name LIKE '%%' || UPPER(%s) || '_' || UPPER(%s) || '_SQ'
                """,
                [table_name, column_name]
            )
            row = cursor.fetchone()
            return row[0] if row else False

    def _drop_identity(self, table_name, column_name):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT sequence_name
                FROM user_sequences 
                WHERE sequence_name LIKE '%%' || %s || '_' || %s || '_SQ'
                """,
                [table_name.upper(), column_name.upper()]
            )
            sequence_name = cursor.fetchone()[0]

            cursor.execute("DROP SEQUENCE %s" % sequence_name)

    # TODO: default_collation은 tibero6와 7에 없는 column입니다. 아래 메서드가 무엇을 위한 것인지 파악하고
    #       수정하기. 그런데 희안하게 모든 Django test에서 이 메서드로 인해 에러가 발생한 경우는 아직 찾지 못했습니다.
    def _get_default_collation(self, table_name):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT default_collation FROM user_tables WHERE table_name = %s
                """,
                [self.normalize_name(table_name)],
            )
            return cursor.fetchone()[0]

    def _collate_sql(self, collation, old_collation=None, table_name=None):
        if collation is None and old_collation is not None:
            collation = self._get_default_collation(table_name)
        return super()._collate_sql(collation, old_collation, table_name)

    # TODO: 아래 메서드는 Django에 존재하지 않고 Tibero를 위해 생성된 메서드입니다. Tibero 6 CS2005
    #       가 Django의 모든 backend는 identity column을 지원함과 동시에 그에 맞게
    #       Django Framework의 코드가 자성된 것을 확인했습니다. Identity column 대신에 sequence를
    #       이용해 autofield를 구현했을 경우 migration에서 잠재적으로 발생할 수 있는 버그가 있을 것으로
    #       판단되어 GENERATED [BY DEFAULT ON NULL | AS ALWAYS]가 지원되는 티베로 버전만 사용할
    #       경우 삭제해야하는 코드입니다.
    def _add_sequence_to_deferred_sql_list_if_autofield(self, model, new_field):
        if new_field.get_internal_type() in (
                "AutoField",
                "BigAutoField",
                "SmallAutoField",
        ):
            autoinc_sql = self.connection.ops.autoinc_sql(
                model._meta.db_table, new_field.column
            )
            if autoinc_sql:
                self.deferred_sql.extend(autoinc_sql)
