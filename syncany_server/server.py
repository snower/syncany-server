# -*- coding: utf-8 -*-
# 2023/5/4
# create by: snower

import os
from sqlglot import expressions as sqlglot_expressions
from mysql_mimic import Session, MysqlServer
from mysql_mimic.results import infer_type
from syncany.logger import get_logger
from syncany.taskers.manager import TaskerManager
from syncany.database.memory import MemoryDBCollection
from syncanysql.compiler import Compiler
from syncanysql.taskers.query import QueryTasker
from syncanysql import ScriptEngine, Executor, ExecuterContext, SqlSegment, SqlParser, ExecuterError
from syncanysql.parser import FileParser
from .user import UserIdentityProvider
from .database import DatabaseManager, Database
from .table import Table


class ServerSessionExecuterContext(ExecuterContext):
    def __init__(self, *args, **kwargs):
        self.session = kwargs.pop("session", None)
        super(ServerSessionExecuterContext, self).__init__(*args, **kwargs)

        self.memory_database_collection = MemoryDBCollection()

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
        self.session.execute_index += 1
        with self.executor as executor:
            executor.run("session[%d_%d]" % (id(self), self.session.execute_index), sqls)
            exit_code = executor.execute()
            if exit_code is not None and exit_code != 0:
                raise ExecuterError(exit_code)
        return 0

    def execute_segments(self, sql_segments):
        self.session.execute_index += 1
        with self.executor as executor:
            executor.run("session[%d_%d]" % (id(self), self.session.execute_index), sql_segments)
            exit_code = executor.execute()
            if exit_code is not None and exit_code != 0:
                raise ExecuterError(exit_code)
        return 0

    def execute_file(self, filename):
        sql_parser = FileParser(filename)
        sqls = sql_parser.load()
        self.session.execute_index += 1
        with self.executor as executor:
            executor.run("session[%s-%d]%s" % (id(self), self.session.execute_index, filename), sqls)
            exit_code = executor.execute()
            if exit_code is not None and exit_code != 0:
                raise ExecuterError(exit_code)
        return 0

    def execute_expression(self, expression, output_name=None):
        self.session.execute_index += 1
        with self.executor as executor:
            config = executor.session_config.get()
            config["name"] = "session[%s-%d]" % (id(self), self.session.execute_index)
            try:
                compiler = Compiler(config, executor.env_variables)
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
            finally:
                config["name"] = ""
            if output_name and isinstance(tasker, QueryTasker):
                tasker.config["output"] = "&." + output_name + "::" + tasker.config["output"].split("::")[-1]
            executor.runners.extend(tasker.start(config["name"], executor, executor.session_config, executor.manager,
                                                 arguments))
            exit_code = executor.execute()
            if exit_code is not None and exit_code != 0:
                raise ExecuterError(exit_code)
        return 0


class ServerSession(Session):
    def __init__(self, executer_context, *args, **kwargs):
        super(ServerSession, self).__init__(*args, **kwargs)

        self.executer_context = executer_context
        self.execute_index = 0
        self.databases = None

    async def init(self, connection):
        await super(ServerSession, self).init(connection)
        if self.databases is None:
            self.load_databases()

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

    async def query(self, expression, sql, attrs):
        if not isinstance(expression, (sqlglot_expressions.Insert, sqlglot_expressions.Delete,
                                       sqlglot_expressions.Select, sqlglot_expressions.Union)):
            return [], []
        if "performance_schema" in sql:
            return [], []

        with self.executer_context.context(self) as executer_context:
            where_variable_sqls = self.parse_where_variable_sqls(expression)
            if where_variable_sqls:
                executer_context.execute_segments([SqlSegment(where_variable_sqls[i], i + 1)
                                                   for i in range(len(where_variable_sqls))])

            database_tables = self.parse_database_tables(expression)
            for database_name, table_name, table_expression in database_tables:
                database_name = database_name or self.database
                database = self.databases[database_name] if database_name in self.databases else None
                if not database:
                    continue
                table = database.get_table(table_name)
                if not table:
                    continue
                executer_context.execute_file(table.filename)
                table_expression.args["db"] = None

            if isinstance(expression, sqlglot_expressions.Insert):
                database_name, table_name = self.parse_insert_table(expression)
                executer_context.execute_expression(expression)
                if (database_name is None or database_name in self.databases) and table_name:
                    datas = executer_context.pop_memory_datas(table_name)
                    if datas:
                        self.executer_context.memory_database[table_name] = datas
                return [], []
            if isinstance(expression, sqlglot_expressions.Delete):
                executer_context.execute_expression(expression)
                return [], []

            collection_name = "__session_execute_%d_%d" % (id(self), self.execute_index + 1)
            executer_context.execute_expression(expression, "--." + collection_name)
            datas = executer_context.pop_memory_datas(collection_name)
            if not datas:
                return [], []
            keys = list(datas[0].keys())
            return [tuple(data[key] for key in keys) for data in datas], keys

    async def schema(self):
        if self.databases is None:
            self.load_databases()
        return {name: {table.name: table.schema for table in database.tables if table.schema}
                for name, database in self.databases.items()}

    async def use(self, database):
        if self.databases is None:
            self.load_databases()
        await super(ServerSession, self).use(database)
        self.executer_context.memory_database_collection.clear()

    def parse_database_tables(self, expression):
        tables = []
        if isinstance(expression, sqlglot_expressions.Select):
            from_expression = expression.args.get("from")
            if from_expression and from_expression.args.get("expressions"):
                table_expression = from_expression.args["expressions"][0]
                if isinstance(table_expression, sqlglot_expressions.Table):
                    tables.append((table_expression.args["db"].name if table_expression.args.get("db") else None,
                                   table_expression.args["this"].name, table_expression))
            joins_expression = expression.args.get("joins") or []
            for join_expression in joins_expression:
                table_expression = join_expression.args["this"]
                if isinstance(table_expression, sqlglot_expressions.Table):
                    tables.append((table_expression.args["db"].name if table_expression.args.get("db") else None,
                                   table_expression.args["this"].name, table_expression))
        if isinstance(expression, sqlglot_expressions.Insert):
            if isinstance(expression.args["expression"], (sqlglot_expressions.Select, sqlglot_expressions.Union)):
                tables.extend(self.parse_database_tables(expression.args["expression"]))
        if isinstance(expression, sqlglot_expressions.Union):
            tables.extend(self.parse_database_tables(expression.args["this"]))
            tables.extend(self.parse_database_tables(expression.args["expression"]))
        return list(set(tables))

    def parse_insert_table(self, expression):
        if not isinstance(expression, sqlglot_expressions.Insert):
            return None, None
        if isinstance(expression.args["this"], sqlglot_expressions.Table):
            table_expression = expression.args["this"]
            return ((table_expression.args["db"].name if "db" in table_expression.args else None),
                    table_expression.args["this"].name)
        return None, None

    def parse_where_variable_sqls(self, expression):
        if not isinstance(expression, sqlglot_expressions.Select):
            return {}
        where_expression = expression.args.get("where")
        if not where_expression:
            return {}
        where_variable_sqls = []

        def parse_where_condition(condition_expression):
            if isinstance(condition_expression, sqlglot_expressions.And):
                parse_where_condition(condition_expression.args.get("this"))
                parse_where_condition(condition_expression.args.get("expression"))
            elif isinstance(condition_expression, sqlglot_expressions.EQ):
                if isinstance(condition_expression.args["this"], sqlglot_expressions.Column):
                    name = condition_expression.args["this"].name
                    where_variable_sqls.append("SELECT %s as %s INTO @%s" % (str(condition_expression.args["expression"]),
                                                                             name, name))
            elif isinstance(condition_expression, (sqlglot_expressions.GT, sqlglot_expressions.GTE, sqlglot_expressions.LT,
                                                   sqlglot_expressions.LTE, sqlglot_expressions.NEQ)):
                if isinstance(condition_expression.args["this"], sqlglot_expressions.Column):
                    name = "%s__%s" % (condition_expression.args["this"].name,condition_expression.key.lower())
                    where_variable_sqls.append("SELECT %s as %s INTO @%s" % (str(condition_expression.args["expression"]),
                                                                             name, name))
        parse_where_condition(where_expression.args["this"])
        return where_variable_sqls if where_variable_sqls else None

    def load_databases(self):
        self.databases = {}
        basepath = os.path.abspath(".")
        for dirname in os.listdir(basepath):
            if not os.path.isdir(dirname):
                continue

            tables = []
            for filename in os.listdir(os.path.join(basepath, dirname)):
                table_name, fileext = os.path.splitext(filename)
                if fileext not in (".sql", ".sqlx"):
                    continue
                filename = os.path.join(basepath, dirname, filename)
                try:
                    sql_parser = FileParser(filename)
                    sqls = sql_parser.load()
                    executor = Executor(self.executer_context.engine.manager,
                                        self.executer_context.executor.session_config.session(), self.executer_context.executor)
                    executor.run("session[%d]" % id(self), sqls)
                    if not executor.runners:
                        continue
                    for tasker in executor.runners:
                        if not isinstance(tasker, QueryTasker):
                            continue
                        if ("&.--." + table_name) in tasker.config["output"]:
                            table_name = tasker.config["output"].split("&.--.")[-1].split("::")[0]
                            tables.append(Table(table_name, filename, self.load_table_schema(tasker)))
                        tasker.tasker.close()
                except Exception as e:
                    get_logger().warning("load database file error %s %s", filename, str(e))
            if not tables:
                continue
            self.databases[dirname] = Database(dirname, tables)

    def load_table_schema(self, tasker):
        schema = {}
        for name, valuer in tasker.tasker.outputer.schema.items():
            final_filter = valuer.get_final_filter()
            schema[name] = infer_type("") if not final_filter else infer_type(final_filter.filter(None))
        return schema


class Server(MysqlServer):
    def __init__(self):
        super(Server, self).__init__(session_factory=self.create_session, identity_provider=UserIdentityProvider())

        self.script_engine = None

    def create_session(self, *args, **kwargs):
        if not self.script_engine:
            return None
        executor = Executor(self.script_engine.manager, self.script_engine.executor.session_config.session(),
                            self.script_engine.executor)
        return ServerSession(ServerSessionExecuterContext(self.script_engine, executor), *args, **kwargs)

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
                exit_code = executor.execute()
                if exit_code is not None and exit_code != 0:
                    raise ExecuterError(exit_code)

    async def start_server(self, **kwargs):
        await super(Server, self).start_server(**kwargs)
        self.setup_script_engine()

    def close(self):
        super(Server, self).close()

        if self.script_engine:
            self.script_engine.close()
        self.script_engine = None
