#######################################################################
# Adapter Definition - SMTP Protocol Class
#
# Notes:
# This class provides access to an SMTP adapter
#
#######################################################################
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class SMTP():

    # Class constructor
    def __init__(self, username:str, password:str, smtp_host:str, smtp_port):
        self._username = username
        self._password = password
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port

    # Sending email
    def send_email(self, sender_email:str, recipient_email:str, subject:str, text_message:str, html_message:str):
        context = ssl.create_default_context()

        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = recipient_email

        part1 = MIMEText(text_message, "plain")
        part2 = MIMEText(html_message, "html")
        message.attach(part1)
        message.attach(part2)

        server = smtplib.SMTP(self._smtp_host,self._smtp_port)
        server.ehlo() # Can be omitted
        server.starttls(context=context) # Secure the connection
        server.ehlo() # Can be omitted
        server.login(self._username, self._password)

        server.login(self._username, self._password)
        server.sendmail(sender_email, recipient_email, message.as_string())

        return True
