from requests_ntlm import HttpNtlmAuth
import os
import requests

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
