#######################################################################
# Use Case Definition - Security Helper File
#
# Notes:
# None
#
#######################################################################
import datetime
import time
import math
import hashlib
import random
import string
import pyotp
import qrcode
import re
from decimal import Decimal

class Security():

    # Class constructor
    def __init__(self):
        self._issuer = 'Arbitrage Platform'

    # Generates an MD5 hash from the value
    def md5_hash(self, value:string):
        return hashlib.md5(value.encode()).hexdigest()

    # Generates an SHA1 hash from the value
    def sha256_hash(self, value:string):
        return hashlib.sha256(value.encode()).hexdigest()

    # Generates an MD5 hash from the value
    def generate_password(self):
        password = ''.join(random.choice(string.ascii_letters + string.digits + '#$%&!@') for i in range(10))
        return password

    # Generates a random salt value
    def generate_salt(self):
        return random.randrange(1000000, 9999999)

    # Generate password hash from clear-text password and salt
    def generate_password_hash(self, password:string, salt:string):
        return self.sha256_hash(f"{salt}-{password}/{salt}")

    # Generate a Google Authenticator QR Code
    def generate_qr_code(self, email:str):
        if bool(email):
            secret = pyotp.random_base32()
            otp_secret_url = pyotp.totp.TOTP(secret).provisioning_uri(email, issuer_name=self._issuer)
            qr = qrcode.QRCode(
                                version=1,
                                error_correction=qrcode.constants.ERROR_CORRECT_L,
                                box_size=10,
                                border=0,
                              )
            qr.add_data(otp_secret_url)
            qr.make(fit=True)
            image = qr.make_image(fill_color="black", back_color="white")
            image.save(f"tmp/{secret}.png")

            return secret

        return False

    # Validates the OTP value
    def validate_otp(self, otp_secret:str, otp_value:str):
        if bool(otp_secret) and bool(otp_value):
            totp = pyotp.TOTP(otp_secret)
            return totp.verify(otp_value)
        return False

    # Validate email address
    def validate_email(self, email:str):
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if(re.fullmatch(regex, email)):
            return True
        return False
