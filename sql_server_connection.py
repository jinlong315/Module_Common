import pyodbc
from urllib.parse import quote_plus
import pymssql
import sqlalchemy




class MSSQL:
	"""
	Create connection to SQL Server
	"""

	def __init__(self, server, database, user, password):
		"""
		Initialization for attributes
		:param server: str | server name
		:param database: str | database name
		:param user: str | username
		:param password: str | password
		"""

		self.server = server
		self.database = database
		self.user = user
		self.password = password

	def con_pyodbc(self):
		"""
		Connection with pyodbc
		:return: object
		"""

		# define connection string
		connection_string = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password}'

		# connection to SQL Server
		con_pyodbc = pyodbc.connect(connection_string, fast_executemany=True)

		return con_pyodbc

	def con_sqlalchemy(self):
		"""
		Connection with sqlalchemy
		:return: object
		"""

		# encode password
		encoded_password = quote_plus(string=self.password)

		# create engine
		engine = sqlalchemy.create_engine(
				f"mssql+pyodbc://{self.user}:{encoded_password}@{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server",
				fast_executemany=True)

		# connection to SQL Server
		con_sqlalchemy = engine.connect()

		return con_sqlalchemy

	def add_table_property(self, table_name, table_desc):
		"""
		:param table_name: table name in MS SQL Server
		:param table_desc: table desc in MS SQL Server
		:return:
		"""

		# create sql string
		sql = f"""
        IF NOT EXISTS (
            SELECT 1
            FROM fn_listextendedproperty (NULL, 'SCHEMA', 'dbo', 'TABLE', '{table_name}', NULL, NULL) 
            WHERE name = 'MS_Description'
        )
        
        BEGIN
        
        EXEC sp_addextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
                
        END
                
        """

		# get cursor
		con = self.con_pyodbc()
		cursor = con.cursor()

		# execute sql string
		cursor.execute(sql)

		# submit and close
		con.commit()
		con.close()

	def update_table_property(self, table_name, table_desc):
		"""
		:param table_name: table name in MS SQL Server
		:param table_desc: table desc in MS SQL Server
		:return:
		"""

		# create sql string
		sql = f"""
        IF EXISTS (
            SELECT 1
            FROM fn_listextendedproperty (NULL, 'SCHEMA', 'dbo', 'TABLE', '{table_name}', NULL, NULL) 
            WHERE name = 'MS_Description'
        )
        
        BEGIN
        
        EXEC sp_updateextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
                
        END
        """

		# get cursor
		con = self.con_pyodbc()
		cursor = con.cursor()

		# execute sql string
		cursor.execute(sql)

		# submit and close
		con.commit()
		con.close()

	def execute_sql_query(self, sql):
		"""
		:param sql: sql query string
		:return: None
		"""

		# get cursor
		con = self.con_pyodbc()
		cursor = con.cursor()

		# execute sql query
		cursor.execute(sql)

		# commit and close
		con.commit()
		con.close()

	def execute_sql_stored_procedure(self, stored_procedure):
		"""
		Execute stored procedure in SQL Server
		:param stored_procedure: string | name of stored procedure
		:return: None
		"""

		# get cursor
		con = self.con_pyodbc()
		cursor = con.cursor()

		# sql string
		sql_string = "{"f"CALL {stored_procedure}""}"

		# execute sql query
		cursor.execute(sql_string)

		# commit and close
		con.commit()
		con.close()

	def truncate_table(self, table_name):
		"""
		Truncate table
		:param table_name: str | table name
		:return: None
		"""

		sql_truncate_table = f"""
        IF OBJECT_ID(QUOTENAME('dbo') + '.' + QUOTENAME('{table_name}'), 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE {table_name};
        END
        """

		# get cursor
		con = self.con_pyodbc()
		cursor = con.cursor()

		# execute sql query
		cursor.execute(sql_truncate_table)

		# commit and close
		con.commit()
		con.close()

	def drop_table(self, table_name):
		"""
		Drop table
		:param table_name: str | table name
		:return: None
		"""

		sql_drop_table = f"""
        IF OBJECT_ID(QUOTENAME('dbo') + '.' + QUOTENAME('{table_name}'), 'U') IS NOT NULL
        BEGIN
            DROP TABLE {table_name};
        END
        """

		# get cursor
		con = self.con_pyodbc()
		cursor = con.cursor()

		# execute sql query
		cursor.execute(sql_drop_table)

		# commit and close
		con.commit()
		con.close()

	def create_table(self, table_name, dict_columns):
		"""
		Create table based on given table name and columns
		:param table_name: str | table name
		:param dict_columns: dict | column name and data type
		:return: None
		"""

		# 构造SQL语句中的列名及数据类型
		str_column = ""
		for k, v in dict_columns.items():
			_ = "".join(["[", k, "]", " ", v])
			str_column = "".join([str_column, _, ","])

		# create sql
		sql_create_table = f"""
        IF OBJECT_ID(QUOTENAME('dbo') + '.' + QUOTENAME('{table_name}'), 'U') IS NOT NULL

        BEGIN
            DROP TABLE {table_name};
        END
        
        CREATE TABLE {table_name} {"("}{str_column[:-1]}{")"}
        """

		# get cursor
		con = self.con_pyodbc()
		cursor = con.cursor()

		# execute sql query
		cursor.execute(sql_create_table)

		# commit and close
		con.commit()
		con.close()
