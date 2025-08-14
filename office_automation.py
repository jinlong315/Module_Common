import comtypes.client
import win32com.client as win32


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
