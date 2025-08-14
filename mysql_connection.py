from urllib.parse import quote_plus
import sqlalchemy
import pymysql

class MySQL:
	"""
	define one object to get connection with MS SQL Server
	"""

	def __init__(self, server, user, password, database, port):
		"""
		:param server: str | host name of MySQL
		:param user: str | user to log in MySQL
		:param password: str | password to log in MySQL
		:param database: str | database name in MySQL
		:param port: str | database name in MySQL
		"""

		self.server = server
		self.user = user
		self.database = database
		self.password = password
		self.port = port

	def sqlalchemy_connection(self):
		"""
		:return: Connect to MySQL
		"""

		# encode password
		encoded_password = quote_plus(string=self.password)

		# create connection to MySQL
		con = sqlalchemy.create_engine(
				f"mysql+pymysql://{self.user}:{encoded_password}@{self.server}:{self.port}/{self.database}")

		return con
