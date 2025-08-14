import logging
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
