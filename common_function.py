import datetime
import pyodbc
import smtplib
import io
import msoffcrypto
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.header import Header
from email.utils import formataddr
import logging
from pathlib import Path
import math
import sqlalchemy
import pandas as pd
import base64
import pymysql
import comtypes.client
import win32com.client as win32
import os
from requests_ntlm import HttpNtlmAuth
import requests
from urllib.parse import quote_plus


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


class SendEmail:
	"""
	One object to send email
	"""

	def __init__(self, sender_name, sender_address, receiver, cc, subject, content):
		"""
		Initialization for attributes
		:param sender_name:  str | sender_name,
		:param sender_address:  str | sender_address,
		:param receiver: list | email address of receiver,
		:param cc: list | email address of Cc,
		:param subject: str | email title
		:param content: str | content of email, can only be normal text
		"""
		self.sender_name = sender_name
		self.sender_address = sender_address
		self.receiver = receiver
		self.cc = cc
		self.subject = subject
		self.content = content

	def send_email_with_text(self):
		# create connection to Email Serve
		email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

		# create email object
		msg = MIMEMultipart()

		# create subject
		title = Header(s=self.subject, charset="utf-8").encode()
		msg["Subject"] = title

		# set sender
		msg["From"] = formataddr((self.sender_name, self.sender_address))

		# set receiver
		msg["To"] = ",".join(self.receiver)

		# set Cc
		msg["Cc"] = ",".join(self.cc)

		# add content
		text = MIMEText(_text=self.content, _subtype="plain", _charset="utf-8")
		msg.attach(text)

		# extend receiver list
		to_list = msg["To"].split(",")
		cc_list = msg["Cc"].split(",")
		to_list.extend(cc_list)

		# send email
		email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
		email_server.quit()

	def send_email_with_html(self, image_path=None):
		# create connection to Email Serve
		email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

		# create email object
		msg = MIMEMultipart()

		# create subject
		title = Header(s=self.subject, charset="utf-8").encode()
		msg["Subject"] = title

		# set sender
		msg["From"] = formataddr((self.sender_name, self.sender_address))

		# set receiver
		msg["To"] = ",".join(self.receiver)

		# set Cc
		msg["Cc"] = ",".join(self.cc)

		# add content
		html = MIMEText(_text=self.content, _subtype="html", _charset="utf-8")
		msg.attach(html)

		# add picture into content
		if image_path:
			for i, path in enumerate(image_path):
				with open(file=path, mode="rb") as img_file:
					img = MIMEImage(img_file.read())
					img.add_header('Content-ID', f'<image{i}>')
					msg.attach(img)

		# extend receiver list
		to_list = msg["To"].split(",")
		cc_list = msg["Cc"].split(",")
		to_list.extend(cc_list)

		# send email
		email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
		email_server.quit()


class PBIRS_API:
	"""
	Call REST API to realize interaction with Power BI Resport Server
	"""

	def __init__(self, user_name="SCHAEFFLER\\P3PQ", password="Pq0123456", localhost="p01251735"):
		"""
		Initialization for attributes
		:param user_name: str | admin username of Power BI Report Server
		:param password: str | password of admin username
		:param localhost: str | server name
		"""
		self.user_name = user_name
		self.password = password
		self.localhost = localhost

		# create authority
		self.auth = HttpNtlmAuth(username=self.user_name, password=self.password)

		# create base URL
		self.url_base = f"https://{self.localhost}.schaeffler.com/reports/api/v2.0/"

		# create requests header
		self.header = {"Content-Type": "application/json"}

	def post_cache_refresh_plan(self, plan_id):
		"""
		Execute schedule plan to refresh model
		:param plan_id: str | schedule plan id
		:return: str | result of response
		"""

		# create query string
		query_string = f"CacheRefreshPlans({plan_id})/Model.Execute"

		# concatenate URL
		url_full = os.path.join(self.url_base, query_string)

		# execute requests
		response = requests.post(url=url_full,
		                         auth=self.auth,
		                         headers=self.header,
		                         verify=False)

		# get response code
		status_code = response.status_code

		return status_code

	def get_pbi_reports(self):
		"""
		Get basic information of catalog items
		:return:
		"""

		# create query string
		query_string = f"PowerBIReports"

		# concatenate URL
		url_full = os.path.join(self.url_base, query_string)
		print(url_full)

		# execute requests
		response = requests.get(url=url_full,
		                        auth=self.auth,
		                        headers=self.header,
		                        verify=False)

		# get response code
		status_code = response.status_code

		# get response
		if status_code in [403, 400, 404]:
			result = response.text
			return result

		elif status_code == 200:
			data = response.json()
			return data


class Log:

	def __init__(self, level, dir_log, mode="a"):
		"""
		Initialization for attributes
		:param level: object | loging level
		:param dir_log: path | absolute directory of log file
		:param mode: str | log mode
		"""

		self.level = level
		self.dir_log = dir_log
		self.mode = mode

	def format_configuration(self):
		"""
		Finish basic configuration for log
		:return: object
		"""

		return logging.basicConfig(filename=self.dir_log,
		                           level=self.level,
		                           format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s",
		                           datefmt="%Y-%m-%d %H:%M:%S",
		                           handlers=[logging.FileHandler(filename=self.dir_log, mode=self.mode)])


class PDFData:
	"""
	Deal with PDF data
	"""

	def __init__(self, dir_pdf):
		"""
		:param dir_pdf: absolute directory of PDF file
		"""

		self.dir_pdf = dir_pdf
		self.file_name = Path(dir_pdf).stem
		self.creation_time = datetime.datetime.fromtimestamp(Path(dir_pdf).stat().st_ctime)
		self.last_modified_time = datetime.datetime.fromtimestamp(Path(dir_pdf).stat().st_mtime)

	def convert_to_base64(self):
		"""
		:return: DataFrame with spil string
		"""

		# open PDF file
		with open(file=self.dir_pdf, mode="rb") as file:
			content = file.read()

			# convert to base64
			base64_content = base64.b64encode(content).decode("utf-8")

			# split string
			str_length = len(base64_content)
			step = 100
			loop_count = math.ceil(str_length / step)

			dict_split = {}

			for i in range(0, loop_count, 1):
				str_split = base64_content[i * step: (i + 1) * step]
				dict_split[i] = str_split

			# save data into pandas
			df_pdf = pd.DataFrame.from_dict(data=dict_split, orient="index", columns=["base64"], dtype="str")

			# reset index
			df_pdf.index.name = "base64_order"
			df_pdf.reset_index(drop=False, inplace=True)

			# create DataFrame
			df_file = pd.DataFrame([[self.file_name, self.creation_time, self.last_modified_time, self.dir_pdf]],
			                       index=[i for i in range(0, loop_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "file_directory"]
			                       )

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_pdf], axis=1)

			return df_merged


class OfficeAutomation:
	"""
	Create often used script to handle Office work
	"""

	def __init__(self, dir_src, dir_dst):
		"""
		Initialization for attributes
		:param dir_src: path like | absolute directory of raw data
		:param dir_dst: path like | destination directory of raw data
		"""
		self.dir_src = dir_src
		self.dir_dst = dir_dst

	def pptx_to_pdf(self):
		"""
		Save PPT as PDF
		:return: None
		"""

		# create instance of PPT
		powerpoint = comtypes.client.CreateObject('PowerPoint.Application')

		# visible for PPT
		powerpoint.Visible = True

		# Open PPT
		presentation = powerpoint.Presentations.Open(self.dir_src)

		# save as PDF
		presentation.SaveAs(self.dir_dst, 32)

		# close and quit
		presentation.Close()
		powerpoint.Quit()

	def ppt_to_images(self):
		"""
		Create pictures for each slide
		:return: None
		"""

		# create instance of PPT
		ppt_app = win32.gencache.EnsureDispatch('PowerPoint.Application')

		# visible for PPT
		ppt_app.Visible = True

		# open PPT
		ppt = ppt_app.Presentations.Open(self.dir_src)

		# loop for each slide
		for slide_index in range(1, ppt.Slides.Count + 1):
			# get each slide
			slide = ppt.Slides(slide_index)

			# output for each image
			image_path = os.path.join(self.dir_dst, f"slide_{slide_index}.png")

			# export as image
			slide.Export(image_path, "PNG")

		# close and quit
		ppt.Close()
		ppt_app.Quit()


class DecryptFile:

	def __init__(self, file_path):
		"""
		Initialization for attributes
		:param file_path: str | path of file
		"""

		self.file_path = file_path

	def is_encrypted(self):
		"""
		If the file is encrypted
		:return: boolean | True or False
		"""

		try:

			with open(file=self.file_path, mode="rb") as f:
				return msoffcrypto.OfficeFile(f).is_encrypted()
		except:
			return False

	def decrypted_file(self, password):
		"""
		Read data from Excel based on different situation
		:param password: str | password
		:return: Object
		"""

		# 如果文件被加密
		if self.is_encrypted():

			if not password:
				raise ValueError("文件已加密,需要提供密码")

			decrypted = io.BytesIO()
			with open(file=self.file_path, mode="rb") as f:

				office_file = msoffcrypto.OfficeFile(f)
				office_file.load_key(password=password)
				office_file.decrypt(decrypted)

			return decrypted


		# 如果文件未被加密
		else:
			return self.file_path
