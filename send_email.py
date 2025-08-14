import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.header import Header
from email.utils import formataddr


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
