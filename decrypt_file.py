import msoffcrypto
import io

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
