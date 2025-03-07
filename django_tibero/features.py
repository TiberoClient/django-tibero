from django.db import DatabaseError, ProgrammingError
from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    minimum_database_version = (6,)
    allows_group_by_lob = False
    allows_group_by_select_index = False
    interprets_empty_strings_as_nulls = True
    has_select_for_update = True
    has_select_for_update_nowait = True
    has_select_for_update_skip_locked = True
    has_select_for_update_of = True
    select_for_update_of_column = True
    can_return_columns_from_insert = True
    supports_subqueries_in_group_by = False
    ignores_unnecessary_order_by_in_subqueries = False
    supports_transactions = True
    supports_timezones = False
    has_native_duration_field = True
    # TODO: foreign key에서 deferrable이 지원되면 그 때 True로 바꾸기
    can_defer_constraint_checks = False
    supports_partially_nullable_unique_constraints = False
    # TODO: Tibero에서는 지원하는데 이 기능을 테스트하는 django 코드에서 SET CONSTRAINT ... IMMEDIATE 구문을 활요합니다.
    #       문제는 티베로가 이 구문을 지원하지 않습니다.
    supports_deferrable_unique_constraints = True
    truncates_names = True
    supports_comments = True
    supports_tablespaces = True
    supports_sequence_reset = False
    can_introspect_materialized_views = True
    atomic_transactions = False
    nulls_order_largest = True
    requires_literal_defaults = True
    supports_default_keyword_in_bulk_insert = False
    closed_cursor_error_class = ProgrammingError
    # Select for update with limit can be achieved on Tibero, but not with the
    # current backend.
    supports_select_for_update_with_limit = False
    supports_temporal_subtraction = True
    # Tibero doesn't ignore quoted identifiers case but the current backend
    # does by uppercasing all identifiers.
    ignores_table_name_case = True
    supports_index_on_text_field = False
    create_test_procedure_without_params_sql = """
        CREATE PROCEDURE "TEST_PROCEDURE" AS
            V_I INTEGER;
        BEGIN
            V_I := 1;
        END;
    """
    create_test_procedure_with_int_param_sql = """
        CREATE PROCEDURE "TEST_PROCEDURE" (P_I INTEGER) AS
            V_I INTEGER;
        BEGIN
            V_I := P_I;
        END;
    """
    create_test_table_with_composite_primary_key = """
        CREATE TABLE test_table_composite_pk (
            column_1 NUMBER(11) NOT NULL,
            column_2 NUMBER(11) NOT NULL,
            PRIMARY KEY (column_1, column_2)
        )
    """
    supports_callproc_kwargs = True
    supports_over_clause = True
    supports_paramstyle_pyformat = False
    supports_frame_range_fixed_distance = True
    supports_ignore_conflicts = False
    max_query_params = 2**16 - 1
    supports_partial_indexes = False
    supports_stored_generated_columns = False
    # TODO: Tibero에서 virtual column을 지원하면 그 때 다시 보기
    supports_virtual_generated_columns = False
    can_rename_index = True
    supports_slicing_ordering_in_compound = True
    requires_compound_order_by_subquery = True
    allows_multiple_constraints_on_same_fields = False
    supports_json_field_contains = False
    supports_collation_on_textfield = False
    test_now_utc_template = "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'"
    # TOOD: 테스트를 하면서 티베로에서 실패하는 메서드 추가하기
    django_test_expected_failures = {
        # 5.1.5 기준으로 oracle backend가 실패하는 테스트 케이스 (#23843).
        "annotations.tests.NonAggregateAnnotationTestCase.test_custom_functions",
        "annotations.tests.NonAggregateAnnotationTestCase."
        "test_custom_functions_can_ref_other_functions",
    }
    insert_test_table_with_defaults = (
        "INSERT INTO {} VALUES (DEFAULT, DEFAULT, DEFAULT)"
    )

    # TOOD: 테스트를 하면서 티베로에서 스킵해야 할 테스트 메서드 추가하기
    @cached_property
    def django_test_skips(self):
        skips = {
            "Tibero doesn't support SHA224.": {
                "db_functions.text.test_sha224.SHA224Tests.test_basic",
                "db_functions.text.test_sha224.SHA224Tests.test_transform",
            },
            "Tibero doesn't correctly calculate ISO 8601 week numbering before 1583 (the Gregorian calendar was introduced in 1582).": {
                "db_functions.datetime.test_extract_trunc.DateFunctionTests.test_trunc_week_before_1000",
                "db_functions.datetime.test_extract_trunc.DateFunctionWithTimeZoneTests.test_trunc_week_before_1000",
            },
            "Tibero doesn't support bitwise XOR.": {
                "expressions.tests.ExpressionOperatorTests.test_lefthand_bitwise_xor",
                "expressions.tests.ExpressionOperatorTests.test_lefthand_bitwise_xor_null",
                "expressions.tests.ExpressionOperatorTests.test_lefthand_bitwise_xor_right_null",
            },
            "Tibero requires ORDER BY in row_number, ANSI:SQL doesn't.": {
                "expressions_window.tests.WindowFunctionTests.test_row_number_no_ordering",
                "prefetch_related.tests.PrefetchLimitTests.test_empty_order",
            },
            "Tibero doesn't support changing collations on indexed columns (#33671).": {
                "migrations.test_operations.OperationTests.test_alter_field_pk_fk_db_collation",
            },
            "Tibero doesn't support comparing NCLOB to NUMBER.": {
                "generic_relations_regress.tests.GenericRelationTests.test_textlink_filter",
            },
            "Tibero doesn't support casting filters to NUMBER.": {
                "lookup.tests.LookupQueryingTests.test_aggregate_combined_lookup",
            },
            "Tibero doesn't support JSON type.": {
                "schema.tests.SchemaTests.test_db_default_output_field_resolving",
                "model_fields.test_jsonfield.TestQuerying.test_usage_in_subquery",
            },
            "pyodbc does not hide '-15104: no data found' exceptions raised in database triggers.": {
                "backends.oracle.tests.TransactionalTests.test_hidden_no_data_found_exception"
            },
        }
        return skips

    @cached_property
    def introspected_field_types(self):
        return {
            **super().introspected_field_types,
            "GenericIPAddressField": "CharField",
            "PositiveBigIntegerField": "BigIntegerField",
            "PositiveIntegerField": "IntegerField",
            "PositiveSmallIntegerField": "IntegerField",
            "SmallIntegerField": "IntegerField",
            "TimeField": "DateTimeField",
        }

    # Tibero 6/7 sql reference을 참고하면 collation keyword 자체가 없습니다.
    supports_collation_on_charfield = False

    # TODO: Tibero 7에서도 Django의 요구사항에 맞게 json을 지원할 수 있는 방법이 아직 없습니다.
    #       나중에 json 관련 뷰가 생성되면 json 지원이 될 수 있는지 확인바랍니다.
    @cached_property
    def supports_json_field(self):
        return self.connection.tibero_version >= (100,)

    # TODO: Tibero 7에서도 Django의 요구사항에 맞게 json을 지원할 수 있는 방법이 아직 없습니다.
    #       나중에 json 관련 뷰가 생성되면 json 지원이 될 수 있는지 확인바랍니다.
    @cached_property
    def supports_primitives_in_json_field(self):
        return self.connection.tibero_version >= (100,)

    @cached_property
    def supports_frame_exclusion(self):
        # Tibero 7에서 관련 테스트가 다 실패하는 것을 확인했습니다.
        # 다음과 같은 WINDOWS 함수에서 EXCLUDE가 지원안되는 것을 확인했습니다.
        #   SUM(e."SALARY") OVER (
        #       ORDER BY e."HIRE_DATE" ASC, e."NAME" DESC
        #       ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        #       EXCLUDE TIES
        #   ) AS "SUM_SALARY_COHORT"
        return self.connection.tibero_version >= (100,)

    @cached_property
    def supports_boolean_expr_in_select_clause(self):
        # Tibero 7에서 관련 테스트가 다 실패하는 것을 확인했습니다.
        # 다음과 같은 select 문안 boolean expression이 다 실패하는 것을 확인했습니다.
        #   SELECT 1 = 1 FROM DUAL;
        #   SELECT 1 < 3 FROM DUAL;
        return self.connection.tibero_version >= (100,)

    @cached_property
    def supports_comparing_boolean_expr(self):
        # Tibero 7에서 관련 쿼리가 실행이 안되는 것을 확인했습니다.
        # 다음과 같이 where clause안에 boolean expression이 작동안하는 것을 확ㅇ니했습니다.
        #  SELECT 1 FROM DUAL WHERE (1 = 1) IS NOT NULL;
        return self.connection.tibero_version >= (100,)

    # TODO: 일단은 무조건 지원하게 했습니다.
    #       나중에 실패하는 테스트 보고나서 티베로에 맞게 수정하기
    @cached_property
    def supports_aggregation_over_interval_types(self):
        return self.connection.tibero_version >= (1,)

    @cached_property
    def bare_select_suffix(self):
        return " FROM DUAL"
