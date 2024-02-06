# -*- coding: utf-8 -*-
# 2023/5/4
# create by: snower

import decimal
import json
import sys
import os
import time
import datetime
from collections import defaultdict
import asyncio
from concurrent.futures import ThreadPoolExecutor
from mysql_mimic.types import ColumnType
from sqlglot import expressions as sqlglot_expressions
from sqlglot import dialects as sqlglot_dialects
from sqlglot import parser as sqlglot_parser
from mysql_mimic import Session, MysqlServer
from mysql_mimic.errors import MysqlError, ErrorCode
from mysql_mimic.intercept import expression_to_value
from syncany.logger import get_logger
from syncany.taskers.manager import TaskerManager
from syncany.database.memory import MemoryDBCollection
from syncanysql.compiler import Compiler, AssignParameter
from syncanysql.taskers.query import QueryTasker
from syncanysql import ScriptEngine, Executor, ExecuterContext, SqlSegment, SqlParser
from syncanysql.parser import FileParser
from .user import UserIdentityProvider
from .database import DatabaseManager, Database


class ServerSessionExecuterContext(ExecuterContext):
    def __init__(self, *args, **kwargs):
        self.session = kwargs.pop("session", None)
        super(ServerSessionExecuterContext, self).__init__(*args, **kwargs)

        self.memory_database_collection = MemoryDBCollection()
        self.execting_primary_tables = None

    def context(self, session):
        executor = Executor(self.engine.manager, self.executor.session_config.session(), self.executor)
        executer_context = ServerSessionExecuterContext(self.engine, executor, session=session)
        executer_context.memory_database_collection.update(self.memory_database_collection)
        return executer_context

    def execute(self, sql):
        if not self.session:
            return super(ServerSessionExecuterContext, self).execute(sql)

        if isinstance(sql, str):
            sql_parser = SqlParser(sql)
            sqls = sql_parser.split()
        else:
            sqls = [sql] if not isinstance(sql, list) else sql
        with self.executor as executor:
            executor.run("session[%d-%d]" % (id(self.session), self.session.execute_index), sqls)
            executor.execute()

    def execute_segments(self, sql_segments):
        with self.executor as executor:
            executor.run("session[%d-%d]" % (id(self.session), self.session.execute_index), sql_segments)
            executor.execute()

    def execute_file(self, filename):
        sql_parser = FileParser(filename)
        sqls = sql_parser.load()
        with self.executor as executor:
            executor.run("session[%s-%d]%s" % (id(self.session), self.session.execute_index, filename), sqls)
            executor.execute()

    def execute_expression(self, expression, output_name=None):
        with self.executor as executor:
            name = "session[%s-%d]" % (id(self.session), self.session.execute_index)
            compiler = Compiler(executor.session_config, executor.env_variables, name)
            arguments = {"@verbose": executor.env_variables.get("@verbose", False),
                         "@timeout": executor.env_variables.get("@timeout", 0),
                         "@limit": executor.env_variables.get("@limit", 0),
                         "@batch": executor.env_variables.get("@batch", 0),
                         "@streaming": executor.env_variables.get("@streaming", False),
                         "@recovery": executor.env_variables.get("@recovery", False),
                         "@join_batch": executor.env_variables.get("@join_batch", 10000),
                         "@insert_batch": executor.env_variables.get("@insert_batch", 0),
                         "@primary_order": False}
            tasker = compiler.compile_expression(expression, arguments)
            if output_name and isinstance(tasker, QueryTasker):
                tasker.config["output"] = "&." + output_name + "::" + tasker.config["output"].split("::")[-1]
            executor.runners.extend(tasker.start(name, executor, executor.session_config, executor.manager, arguments))
            executor.execute()


class MySQL(sqlglot_dialects.MySQL):
    class Parser(sqlglot_dialects.MySQL.Parser):
        def _parse_parameter(self):
            wrapped = self._match(sqlglot_parser.TokenType.L_BRACE)
            this = self._parse_var() or self._parse_primary()
            self._match(sqlglot_parser.TokenType.R_BRACE)
            if not self._match_pair(sqlglot_parser.TokenType.COLON, sqlglot_parser.TokenType.EQ, False):
                return self.expression(sqlglot_expressions.Parameter, this=this, wrapped=wrapped)
            self._advance(2)
            expression = self._parse_conjunction() or self._parse_function() or self._parse_id_var()
            return self.expression(AssignParameter, this=this, expression=expression, wrapped=wrapped)

        def _parse_limit(self, this=None, top=False):
            if top or not self._match(sqlglot_parser.TokenType.LIMIT, False):
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            offset_token = sqlglot_parser.seq_get(self._tokens, self._index + 1)
            if offset_token is None or offset_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            comma_token = sqlglot_parser.seq_get(self._tokens, self._index + 2)
            if comma_token is None or comma_token.token_type != sqlglot_parser.TokenType.COMMA:
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            limit_token = sqlglot_parser.seq_get(self._tokens, self._index + 3)
            if limit_token is None or limit_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            return self.expression(sqlglot_parser.exp.Limit, this=this,
                                   expression=self.PRIMARY_PARSERS[sqlglot_parser.TokenType.NUMBER](self, limit_token))

        def _parse_offset(self, this=None):
            if not self._match(sqlglot_parser.TokenType.LIMIT, False):
                return sqlglot_parser.Parser._parse_offset(self, this)
            offset_token = sqlglot_parser.seq_get(self._tokens, self._index + 1)
            if offset_token is None or offset_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_offset(self, this)
            comma_token = sqlglot_parser.seq_get(self._tokens, self._index + 2)
            if comma_token is None or comma_token.token_type != sqlglot_parser.TokenType.COMMA:
                return sqlglot_parser.Parser._parse_offset(self, this)
            limit_token = sqlglot_parser.seq_get(self._tokens, self._index + 3)
            if limit_token is None or limit_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_offset(self, this)
            self._advance(4)
            return self.expression(sqlglot_parser.exp.Offset, this=this,
                                   expression=self.PRIMARY_PARSERS[sqlglot_parser.TokenType.NUMBER](self, offset_token))

    class Generator(sqlglot_dialects.MySQL.Generator):
        TRANSFORMS = {
            **sqlglot_dialects.MySQL.Generator.TRANSFORMS,
            AssignParameter: lambda self, e: self.assign_parameter_sql(e),
        }

        def assign_parameter_sql(self, expression):
            this = self.sql(expression, "this")
            this = f"{{{this}}}" if expression.args.get("wrapped") else f"{this}"
            return f"""{self.PARAMETER_TOKEN}{this} := {self.sql(expression, "expression")}"""


class ServerSession(Session):
    dialect = MySQL

    def __init__(self, config_path, executer_context, identity_provider, thread_pool_executor, databases,
                 executor_wait_timeout, *args, **kwargs):
        super(ServerSession, self).__init__(*args, **kwargs)

        self.loop = asyncio.get_event_loop()
        self.config_path = config_path
        self.executer_context = executer_context
        self.identity_provider = identity_provider
        self.thread_pool_executor = thread_pool_executor
        self.databases = databases
        self.executor_wait_timeout = executor_wait_timeout
        self.execute_index = 0

    async def handle_query(self, sql, attrs):
        if sql[:5].lower() == "show " or sql[:4].lower() == "set " or sql[:5].lower() == "kill " \
                or "information_schema" in sql.lower():
            try:
                return await super(ServerSession, self).handle_query(sql, attrs)
            except:
                sql = sql.lower()
                if "show character set" in sql:
                    return [('utf8mb4', 'UTF-8 Unicode', 'utf8mb4_general_ci', '4')], ('Charset', 'Description',
                                                                                       'Default collation', 'Maxlen')
                if "show engines" in sql:
                    return [('InnoDB', 'DEFAULT', 'Supports transactions, row-level locking, and foreign keys', 'YES',
                             'YES', 'YES')], ('Engine', 'Support', 'Comment', 'Transactions', 'XA', 'Savepoints')
                if "show charset" in sql:
                    return [('utf8', 'UTF-8 Unicode', 'utf8_general_ci', '3')], ['Charset', 'Description',
                                                                                 'Default collation', 'Maxlen']
                if "show collation" in sql:
                    return [('utf8_unicode_ci', 'utf8', '192', '', 'Yes', '8')], ('Collation', 'Charset', 'Id',
                                                                                  'Default', 'Compiled', 'Sortlen')
                if "show procedure status" in sql:
                    return [], ('Db', 'Name', 'Type', 'Definer', 'Modified', 'Created', 'Security_type', 'Comment',
                                'character_set_client', 'collation_connection', 'Database Collation')
                if "show function status" in sql:
                    return [], ('Db', 'Name', 'Type', 'Definer', 'Modified', 'Created', 'Security_type', 'Comment',
                                'character_set_client', 'collation_connection', 'Database Collation')
                if "show table status" in sql:
                    return [], ('Name', 'Engine', 'Version', 'Row_format', 'Rows', 'Avg_row_length', 'Data_length',
                                'Max_data_length', 'Index_length', 'Data_free', 'Auto_increment', 'Create_time',
                                'Update_time', 'Check_time', 'Collation', 'Checksum', 'Create_options', 'Comment')
                return [], []
        return await super(ServerSession, self).handle_query(sql, attrs)

    def _set_variable(self, setitem):
        assignment = setitem.this
        left = assignment.left

        if isinstance(left, sqlglot_expressions.SessionParameter):
            scope = left.text("kind") or "SESSION"
            name = left.name
        elif isinstance(left, sqlglot_expressions.Parameter):
            scope = left.text("kind") or "GLOBAL"
            name = left.name
        else:
            scope = setitem.text("kind") or "SESSION"
            name = left.name

        scope = scope.upper()
        value = expression_to_value(assignment.right)

        if scope in {"SESSION", "LOCAL"}:
            self.variables.set(name, value)
        if self.executer_context:
            with self.executer_context as executer_context:
                try:
                    with executer_context.executor as executor:
                        executor.run("session[%d-%d]" % (id(self), self.execute_index),
                                     [SqlSegment("set " + self.generate_sql(setitem), 1)])
                        executor.execute()
                except Exception as e:
                    raise MysqlError(str(e), code=ErrorCode.NOT_SUPPORTED_YET)
        else:
            raise MysqlError(
                f"Cannot SET variable {name} with scope {scope}",
                code=ErrorCode.NOT_SUPPORTED_YET,
            )

    async def _show_interceptor(self, expression):
        if isinstance(expression, sqlglot_expressions.Show):
            kind = expression.name.upper()
            if kind != 'CREATE TABLE':
                return await super(ServerSession, self)._show_interceptor(expression)
            db_name = expression.args.get("db").name
            if db_name not in self.databases:
                return None
            table_name = expression.args.get("target").name
            table_schema = self.databases[db_name].get_table_schema(table_name)
            if not table_schema:
                return None
            column_sql = []
            for column_name, column_type in table_schema.items():
                if column_type[0] in (ColumnType.VARCHAR,):
                    column_sql.append("`%s` %s(4096)" % (column_name, column_type[0].name))
                else:
                    column_sql.append("`%s` %s" % (column_name, column_type[0].name))
            return [(table_name, "CREATE TABLE `%s` (\n%s\n) DEFAULT CHARSET=utf8mb4" %
                     (table_name, ",\n".join(column_sql)))], ("Table", "Create Table")
        return await super(ServerSession, self)._show_interceptor(expression)

    async def _static_query_interceptor(self, expression):
        try:
            if isinstance(expression, sqlglot_expressions.Select) and \
                    not any(expression.args.get(a)
                            for a in set(sqlglot_expressions.Select.arg_types) - {"expressions", "limit", "hint"}):
                return await self.query(expression, self.generate_sql(expression), {})
            return None
        except Exception:
            return super(ServerSession, self)._static_query_interceptor(expression)

    async def query(self, expression, sql, attrs):
        if not isinstance(expression, (sqlglot_expressions.Insert, sqlglot_expressions.Delete,
                                       sqlglot_expressions.Select, sqlglot_expressions.Union)):
            if sql.lower().startswith("flush"):
                await self.loop.run_in_executor(self.thread_pool_executor, self.identity_provider.load_users)
                await self.loop.run_in_executor(self.thread_pool_executor, Database.scan_databases,
                                                self.config_path, self.executer_context.engine, self.databases)
                return [(database.name, table.name, table.filename) for database in self.databases.values()
                        for table in database.tables], ["database", "table", "filename"]
            return [], []
        if "performance_schema" in sql:
            return [], []
        start_time = time.time()
        try:
            self.execute_index += 1
            get_logger().info("session[%d-%d] query SQL: %s", id(self), self.execute_index, sql.replace("\n", " "))
            result = await self.loop.run_in_executor(self.thread_pool_executor, self.execute_query, expression, start_time)
            if isinstance(result, Exception):
                raise result
            return result
        finally:
            get_logger().info("session[%d-%d] query SQL finish %.2fms", id(self), self.execute_index,
                              (time.time() - start_time) * 1000)

    def execute_query(self, expression, start_time):
        executor_wait_timeout = self.variables.values.get("wait_timeout") or self.executor_wait_timeout
        if start_time + int(executor_wait_timeout) <= time.time():
            raise TimeoutError("query execute wait timeout")

        if expression.args.get("into"):
            with self.executer_context as executer_context:
                executer_context.session = self
                try:
                    executer_context.execute_expression(expression)
                finally:
                    executer_context.session = None
            return [], []

        with self.executer_context.context(self) as executer_context:
            executer_context.execting_primary_tables = self.parse_primary_tables(expression, defaultdict(list))
            if executer_context.execting_primary_tables:
                self.execute_tables(executer_context, executer_context.execting_primary_tables,
                                    self.parse_primary_variable_sqls(expression))
            joins_tables = self.parse_join_tables(expression, defaultdict(list))
            if joins_tables:
                self.execute_tables(executer_context, joins_tables, self.parse_joins_variable_sqls(expression))

            if isinstance(expression, sqlglot_expressions.Insert):
                database_name, table_name = self.parse_insert_table(expression)
                if self.identity_provider.is_readonly(self.username):
                    if not self.identity_provider.has_permission(self.username, "temporary_memory_table"):
                        raise MysqlError("no temporary_memory_table permission", code=ErrorCode.ACCESS_DENIED_ERROR)
                    if database_name and database_name not in ("-", "--"):
                        raise MysqlError("readonly", code=ErrorCode.ACCESS_DENIED_ERROR)
                    if not table_name or table_name in (".txt", ".csv", ".json") or table_name.lower().startswith("file://") \
                            or os.path.splitext(os.path.split(table_name)[-1])[-1] in (".txt", ".json", ".csv", ".xls", ".xlsx"):
                        raise MysqlError("readonly", code=ErrorCode.ACCESS_DENIED_ERROR)
                executer_context.execute_expression(expression)
                if (database_name is None or database_name in self.databases) and table_name:
                    datas = executer_context.pop_memory_datas(table_name)
                    if datas:
                        self.executer_context.memory_database_collection[table_name] = datas
                return [], []
            if isinstance(expression, sqlglot_expressions.Delete):
                executer_context.execute_expression(expression)
                return [], []

            collection_name = "__session_execute_%d_%d" % (id(self), self.execute_index)
            executer_context.execute_expression(expression, "--." + collection_name)
            datas = executer_context.pop_memory_datas(collection_name)
            if not datas:
                return [], []
            keys = list(datas[0].keys())

            def format_value(value):
                if value is None:
                    return value
                if isinstance(value, datetime.datetime):
                    return datetime.datetime(value.year, value.month, value.day, value.hour, value.minute, value.second)
                if isinstance(value, (bool, int, float, str, bytes, datetime.date, datetime.time, decimal.Decimal)):
                    return value
                if isinstance(value, (set, map, list, tuple)):
                    return json.dumps(value, default=str, ensure_ascii=False)
                return str(value)
            return [tuple(format_value(data[key]) for key in keys) for data in datas], keys

    async def schema(self):
        user_databases = await self.identity_provider.get_databases(self.username)
        return {name: {table.name: {column_name: column_type[0] for column_name, column_type in table.schema.items()}
                       for table in database.tables if table.schema}
                for name, database in self.databases.items()
                if not user_databases or name in user_databases}

    async def use(self, database):
        await super(ServerSession, self).use(database)
        self.executer_context.memory_database_collection.clear()

    def execute_tables(self, executer_context, tables, variable_sqls):
        for (database_name, table_name), table_expressions in tables.items():
            database = self.databases[database_name or self.database] if database_name or self.database else None
            if not database:
                continue
            table = database.get_table(table_name)
            if not table:
                continue
            with Executor(executer_context.engine.manager, executer_context.executor.session_config.session(),
                          executer_context.executor) as executor:
                table_variable_sqls = variable_sqls.get((database_name, table_name))
                if table_variable_sqls:
                    executor.run("session[%d-%d]" % (id(self), self.execute_index),
                                 [SqlSegment(table_variable_sqls[i], i + 1) for i in range(len(table_variable_sqls))])
                    executor.execute()

                sql_parser = FileParser(table.filename)
                sqls = sql_parser.load()
                executor.run("session[%s-%d]%s" % (id(self), self.execute_index, table.filename), sqls)
                executor.execute()

            for table_expression in table_expressions:
                table_expression.args["db"] = None

    def parse_primary_tables(self, expression, tables):
        if isinstance(expression, sqlglot_expressions.Select):
            from_expression = expression.args.get("from")
            if from_expression and from_expression.args.get("expressions"):
                table_expression = from_expression.args["expressions"][0]
                if isinstance(table_expression, sqlglot_expressions.Table):
                    database_name = table_expression.args["db"].name if table_expression.args.get("db") else None
                    table_name = table_expression.args["this"].name
                    if self.databases.get(database_name or self.database) \
                            and self.databases.get(database_name or self.database).get_table(table_name):
                        tables[database_name, table_name].append(table_expression)
        if isinstance(expression, sqlglot_expressions.Insert):
            if isinstance(expression.args["expression"], (sqlglot_expressions.Select, sqlglot_expressions.Union)):
                self.parse_primary_tables(expression.args["expression"], tables)
        if isinstance(expression, sqlglot_expressions.Union):
            self.parse_primary_tables(expression.args["this"], tables)
            self.parse_primary_tables(expression.args["expression"], tables)
        return tables

    def parse_join_tables(self, expression, tables):
        if isinstance(expression, sqlglot_expressions.Select):
            joins_expression = expression.args.get("joins") or []
            for join_expression in joins_expression:
                table_expression = join_expression.args["this"]
                if isinstance(table_expression, sqlglot_expressions.Table):
                    database_name = table_expression.args["db"].name if table_expression.args.get("db") else None
                    table_name = table_expression.args["this"].name
                    if self.databases.get(database_name or self.database) \
                            and self.databases.get(database_name or self.database).get_table(table_name):
                        tables[database_name, table_name].append(table_expression)
        if isinstance(expression, sqlglot_expressions.Insert):
            if isinstance(expression.args["expression"], (sqlglot_expressions.Select, sqlglot_expressions.Union)):
                self.parse_join_tables(expression.args["expression"], tables)
        if isinstance(expression, sqlglot_expressions.Union):
            self.parse_join_tables(expression.args["this"], tables)
            self.parse_join_tables(expression.args["expression"], tables)
        return tables

    def parse_insert_table(self, expression):
        if not isinstance(expression, sqlglot_expressions.Insert):
            return None, None
        if isinstance(expression.args["this"], sqlglot_expressions.Schema):
            expression = expression.args["this"]
        if isinstance(expression.args["this"], sqlglot_expressions.Table):
            table_expression = expression.args["this"]
            return ((table_expression.args["db"].name if table_expression.args.get("db") else None),
                    table_expression.args["this"].name)
        return None, None

    def parse_primary_variable_sqls(self, expression):
        if not isinstance(expression, sqlglot_expressions.Select):
            return defaultdict(list)
        primary_variable_sqls = defaultdict(list)

        def parse_primary_condition(database_name, table_name, table_alias, condition_expression):
            if isinstance(condition_expression, sqlglot_expressions.And):
                parse_primary_condition(database_name, table_name, table_alias, condition_expression.args.get("this"))
                parse_primary_condition(database_name, table_name, table_alias, condition_expression.args.get("expression"))
            elif isinstance(condition_expression, sqlglot_expressions.EQ):
                if not isinstance(condition_expression.args["this"], sqlglot_expressions.Column):
                    return
                if self.has_column(condition_expression.args["expression"]):
                    return
                if "table" in condition_expression.args["this"].args:
                    condition_table_name = condition_expression.args["this"].args["table"].name
                    if condition_table_name and condition_table_name != table_alias:
                        return
                name = condition_expression.args["this"].name
                primary_variable_sqls[(database_name, table_name)].append(
                    "SELECT %s as %s INTO @%s" % (self.generate_sql(condition_expression.args["expression"]), name, name))
            elif isinstance(condition_expression, (sqlglot_expressions.GT, sqlglot_expressions.GTE,
                                                   sqlglot_expressions.LT, sqlglot_expressions.LTE,
                                                   sqlglot_expressions.NEQ)):
                if not isinstance(condition_expression.args["this"], sqlglot_expressions.Column):
                    return
                if self.has_column(condition_expression.args["expression"]):
                    return
                if "table" in condition_expression.args["this"].args:
                    condition_table_name = condition_expression.args["this"].args["table"].name
                    if condition_table_name and condition_table_name != table_alias:
                        return
                name = "%s__%s" % (condition_expression.args["this"].name, condition_expression.key.lower())
                primary_variable_sqls[(database_name, table_name)].append(
                    "SELECT %s as %s INTO @%s" % (self.generate_sql(condition_expression.args["expression"]), name, name))

        database_name, table_name, table_alias = None, None, None
        from_expression = expression.args.get("from")
        if from_expression and from_expression.args.get("expressions"):
            table_expression = from_expression.args["expressions"][0]
            if isinstance(table_expression, sqlglot_expressions.Table):
                database_name = table_expression.args["db"].name if table_expression.args.get("db") else None
                table_name = table_expression.args["this"].name
                table_alias = table_expression.args["alias"].name if table_expression.args.get("alias") else table_name
        if not table_name:
            return primary_variable_sqls

        where_expression = expression.args.get("where")
        if where_expression:
            parse_primary_condition(database_name, table_name, table_alias, where_expression.args["this"])

        order_expression = expression.args.get("order")
        if order_expression:
            order_bys = []
            for order_expression in order_expression.args["expressions"]:
                if not isinstance(order_expression.args["this"], sqlglot_expressions.Column):
                    continue
                if "table" in order_expression.args["this"].args:
                    order_table_name = order_expression.args["this"].args["table"].name
                    if order_table_name and order_table_name != table_name:
                        continue
                order_bys.append(self.generate_sql(order_expression))
            if order_bys:
                primary_variable_sqls[(database_name, table_name)].append(
                    "SELECT '%s' as %s INTO @%s" % (",".join(order_bys), "order_by", "order_by"))

        if expression.args.get("offset"):
            offset_expression, limit_expression = expression.args.get("limit"), expression.args.get("offset")
        else:
            offset_expression, limit_expression = None, expression.args.get("limit")
        if limit_expression:
            primary_variable_sqls[(database_name, table_name)].append(
                "SELECT %d as %s INTO @%s" % (max(int(offset_expression.args["expression"].args["this"]), 0)
                                              if offset_expression else 0, "limit_offset", "limit_offset"))
            primary_variable_sqls[(database_name, table_name)].append(
                "SELECT %d as %s INTO @%s" % (max(int(limit_expression.args["expression"].args["this"]), 1),
                                              "limit_count", "limit_count"))
        return primary_variable_sqls

    def parse_joins_variable_sqls(self, expression):
        if not isinstance(expression, sqlglot_expressions.Select):
            return defaultdict(list)
        joins_expression = expression.args.get("joins") or []
        if not joins_expression:
            return defaultdict(list)
        joins_variable_sqls = defaultdict(list)

        def parse_on_condition(database_name, table_name, table_alias, condition_expression):
            if isinstance(condition_expression, sqlglot_expressions.And):
                parse_on_condition(database_name, table_name, table_alias, condition_expression.args.get("this"))
                parse_on_condition(database_name, table_name, table_alias, condition_expression.args.get("expression"))
            elif isinstance(condition_expression, sqlglot_expressions.EQ):
                if not isinstance(condition_expression.args["this"], sqlglot_expressions.Column):
                    return
                if self.has_column(condition_expression.args["expression"]):
                    return
                if "table" in condition_expression.args["this"].args:
                    condition_table_name = condition_expression.args["this"].args["table"].name
                    if condition_table_name and condition_table_name != table_alias:
                        return
                name = condition_expression.args["this"].name
                joins_variable_sqls[(database_name, table_name)].append(
                    "SELECT %s as %s INTO @%s" % (self.generate_sql(condition_expression.args["expression"]), name, name))
            elif isinstance(condition_expression, (sqlglot_expressions.GT, sqlglot_expressions.GTE,
                                                   sqlglot_expressions.LT, sqlglot_expressions.LTE,
                                                   sqlglot_expressions.NEQ)):
                if not isinstance(condition_expression.args["this"], sqlglot_expressions.Column):
                    return
                if self.has_column(condition_expression.args["expression"]):
                    return
                if "table" in condition_expression.args["this"].args:
                    condition_table_name = condition_expression.args["this"].args["table"].name
                    if condition_table_name and condition_table_name != table_alias:
                        return
                name = "%s__%s" % (condition_expression.args["this"].name, condition_expression.key.lower())
                joins_variable_sqls[(database_name, table_name)].append(
                    "SELECT %s as %s INTO @%s" % (self.generate_sql(condition_expression.args["expression"]), name, name))

        for join_expression in joins_expression:
            table_expression = join_expression.args["this"]
            if not isinstance(table_expression, sqlglot_expressions.Table):
                continue
            if not join_expression.args.get("on"):
                continue
            parse_on_condition(table_expression.args["db"].name if table_expression.args.get("db") else None,
                               table_expression.args["this"].name, table_expression.args["alias"].name
                               if table_expression.args.get("alias") else table_expression.args["this"].name,
                               join_expression.args["on"])
        return joins_variable_sqls

    def has_column(self, expression):
        if not isinstance(expression, sqlglot_expressions.Expression):
            return False
        if isinstance(expression, (sqlglot_expressions.Column, sqlglot_expressions.Select,
                                   sqlglot_expressions.Subquery, sqlglot_expressions.Union)):
            return True

        for name, child_expression in expression.args.items():
            if isinstance(child_expression, list):
                for child_expression_item in child_expression:
                    if self.has_column(child_expression_item):
                        return True
                continue
            if self.has_column(child_expression):
                return True
        return False

    def generate_sql(self, expression):
        if isinstance(expression, sqlglot_expressions.Expression):
            return expression.sql(dialect=MySQL)
        return str(expression)


class Server(MysqlServer):
    origin_parse_table = Compiler.parse_table
    origin_parse_column = Compiler.parse_column
    origin_compile_select_star_column = Compiler.compile_select_star_column

    def __init__(self, host=None, port=3306, config_path=".", username=None, password=None,
                 executor_max_workers=5, executor_wait_timeout=120):
        super(Server, self).__init__(session_factory=self.create_session,
                                     identity_provider=UserIdentityProvider(config_path, username, password))

        self.host = host
        self.port = port
        self.config_path = config_path
        self.executor_max_workers = executor_max_workers
        self.executor_wait_timeout = executor_wait_timeout
        self.script_engine = None
        self.thread_pool_executor = None
        self.databases = {}

    def create_session(self, *args, **kwargs):
        if not self.script_engine:
            return None
        executor = Executor(self.script_engine.manager, self.script_engine.executor.session_config.session(),
                            self.script_engine.executor)
        return ServerSession(self.config_path, ServerSessionExecuterContext(self.script_engine, executor),
                             self.identity_provider, self.thread_pool_executor, self.databases,
                             self.executor_wait_timeout, *args, **kwargs)

    def setup_script_engine(self):
        if self.script_engine is not None:
            return
        self.script_engine = ScriptEngine()
        init_execute_files = self.script_engine.config.load()
        self.script_engine.config.config_logging()
        self.script_engine.config.load_extensions()
        self.script_engine.manager = TaskerManager(DatabaseManager())
        self.script_engine.executor = Executor(self.script_engine.manager, self.script_engine.config.session())
        if init_execute_files:
            self.script_engine.executor.run("init", [SqlSegment("execute `%s`" % init_execute_files[i], i + 1)
                                                     for i in range(len(init_execute_files))])
            with self.script_engine.executor as executor:
                executor.execute()
        self.thread_pool_executor = ThreadPoolExecutor(self.executor_max_workers)
        self.identity_provider.load_users()
        Database.scan_databases(self.config_path, self.script_engine, self.databases)

    async def start_server(self, **kwargs):
        self.setup_script_engine()

        def parse_table(compiler, *args):
            table_info = Server.origin_parse_table(compiler, *args)
            if not isinstance(table_info, dict):
                return table_info
            if not hasattr(compiler, "server_schemas"):
                setattr(compiler, "server_schemas", {})
            compiler.server_schemas[table_info["table_name"]] = table_info
            return table_info

        def parse_column(compiler, *args):
            column_info = Server.origin_parse_column(compiler, *args)
            if not isinstance(column_info, dict):
                return column_info
            if column_info.get("typing_filters") or not column_info.get("table_name"):
                return column_info
            if hasattr(compiler, "server_schemas") and column_info["table_name"] in compiler.server_schemas:
                table_info = compiler.server_schemas[column_info["table_name"]]
                db_name, table_name = table_info["db"], table_info["name"]
            else:
                table_info = column_info["table_name"].split(".")
                if len(table_info) <= 1:
                    return column_info
                db_name, table_name = table_info[0], table_info[1]
            if db_name not in self.databases:
                return column_info
            table_schema = self.databases[db_name].get_table_schema(table_name)
            if table_schema is None:
                return column_info
            if column_info["column_name"] not in table_schema:
                return column_info
            column_info["typing_filters"] = [table_schema[column_info["column_name"]][1]]
            return column_info

        def compile_select_star_column(compiler, expression, config, arguments, primary_table, join_tables):
            if Server.origin_compile_select_star_column(compiler, expression, config, arguments,
                                                        primary_table, join_tables):
                return True
            if isinstance(expression, sqlglot_expressions.Star):
                db_name, table_name = primary_table["db"], primary_table["name"]
                db_table_name = primary_table["table_alias"] or primary_table["table_name"]
            else:
                table_name = expression.args["table"].name
                if table_name == primary_table["table_alias"]:
                    db_name = primary_table["db"]
                else:
                    db_name = join_tables[table_name]["db"] if table_name in join_tables else None
                db_table_name = table_name
            if db_name == "--":
                executer_context = ExecuterContext.current()
                if executer_context and isinstance(executer_context, ServerSessionExecuterContext):
                    if executer_context.execting_primary_tables:
                        for primary_table_db, primary_table_name in executer_context.execting_primary_tables:
                            if primary_table_name == table_name:
                                db_name = primary_table_db
            if not db_name or db_name not in self.databases:
                return False
            table_schema = self.databases[db_name].get_table_schema(table_name)
            if table_schema is None:
                return False
            for name, column in table_schema.items():
                if name in config["schema"]:
                    continue
                column_info = {
                    "table_name": db_table_name,
                    "column_name": name,
                    "origin_name": name,
                    "typing_name": "",
                    "dot_keys": [],
                    "typing_filters": [column[1]] if column and len(column) >= 2 and column[1] else [],
                    "typing_options": [],
                    "expression": expression
                }
                compiler.compile_select_column(expression, config, arguments, primary_table, name,
                                               column_info, join_tables)
            return True

        Compiler.parse_table = parse_table
        Compiler.parse_column = parse_column
        Compiler.compile_select_star_column = compile_select_star_column
        await super(Server, self).start_server(host=self.host, port=self.port,
                                               reuse_port=True if sys.platform != "win32" else None,
                                               backlog=512, **kwargs)

    def close(self):
        super(Server, self).close()

        if self.script_engine:
            self.script_engine.close()
        self.script_engine = None
