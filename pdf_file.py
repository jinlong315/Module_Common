import base64
import math
import pandas as pd

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
