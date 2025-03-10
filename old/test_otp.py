#!/usr/bin/env python3
"""
Test script for OTP generation
"""

import os
import logging
import pyotp
import urllib.parse
import hmac
import hashlib
import time
import struct
from dotenv import load_dotenv
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CustomTOTP(pyotp.TOTP):
    """Custom TOTP implementation that supports SHA512"""
    
    def __init__(self, s, digits=6, digest='sha1', name=None, issuer=None, interval=30):
        """Initialize the TOTP object with custom digest support"""
        self.secret = s
        self.digits = digits
        self.digest = digest.lower()  # Support sha1, sha256, sha512
        self.name = name or 'TOTP'
        self.issuer = issuer
        self.interval = interval
    
    def generate_otp(self, input):
        """Generate the OTP using the specified digest algorithm"""
        if self.digest == 'sha1':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha1)
        elif self.digest == 'sha256':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha256)
        elif self.digest == 'sha512':
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha512)
        else:
            # Default to SHA1 if unknown digest
            hasher = hmac.new(self.byte_secret(), struct.pack('>Q', input), hashlib.sha1)
        
        hmac_hash = bytearray(hasher.digest())
        offset = hmac_hash[-1] & 0x0F
        code = ((hmac_hash[offset] & 0x7F) << 24 |
                (hmac_hash[offset + 1] & 0xFF) << 16 |
                (hmac_hash[offset + 2] & 0xFF) << 8 |
                (hmac_hash[offset + 3] & 0xFF))
        code = code % 10 ** self.digits
        return code

    def now(self):
        """Generate the current time OTP"""
        timecode = int(time.time()) // self.interval
        return self.generate_code(timecode)
    
    def generate_code(self, input):
        """Generate a code using the digit count"""
        result = self.generate_otp(input)
        return str(result).zfill(self.digits)

def generate_otp_code(otp_secret):
    """Generate an OTP code from the given secret"""
    if not otp_secret:
        logging.error("No OTP secret provided")
        return None
    
    logging.info(f"Attempting to generate OTP code from secret: {otp_secret[:15]}...")
    
    try:
        # Check if the input is already a otpauth:// URL
        if otp_secret.startswith('otpauth://'):
            logging.info("Processing otpauth:// URL")
            # Parse the otpauth URL
            parsed_url = urllib.parse.urlparse(otp_secret)
            query_params = dict(urllib.parse.parse_qsl(parsed_url.query))
            
            # Extract the secret from the query parameters
            secret = query_params.get('secret')
            if not secret:
                logging.error("No secret found in otpauth URL")
                return None
            
            logging.info(f"Extracted secret: {secret[:5]}...")
            
            # Get algorithm if specified
            algorithm = query_params.get('algorithm', 'SHA1')
            logging.info(f"Using algorithm: {algorithm}")
            
            # Get digits if specified
            digits = int(query_params.get('digits', 6))
            logging.info(f"Using digits: {digits}")
            
            # Get period if specified
            period = int(query_params.get('period', 30))
            logging.info(f"Using period: {period}")
            
            # Generate the OTP code
            if algorithm.upper() == 'SHA512':
                logging.info("Using custom TOTP implementation for SHA512")
                totp = CustomTOTP(secret, digits=digits, digest='sha512', interval=period)
            else:
                # Use standard PyOTP for other algorithms
                totp = pyotp.TOTP(secret, digits=digits, digest=algorithm.lower(), interval=period)
            
            otp_code = totp.now()
            logging.info(f"Generated OTP code: {otp_code}")
            return otp_code
        # Special case for format like "totp/Telekom:hakan%40ekerfiber.com?secret=..."
        elif 'secret=' in otp_secret:
            logging.info("Processing partial otpauth URL")
            # Extract the secret parameter
            secret_param = otp_secret.split('secret=')[1].split('&')[0]
            logging.info(f"Extracted secret: {secret_param[:5]}...")
            
            # Extract other parameters if available
            digits = 6
            if 'digits=' in otp_secret:
                digits_str = otp_secret.split('digits=')[1].split('&')[0]
                digits = int(digits_str)
                logging.info(f"Using digits: {digits}")
            
            algorithm = 'SHA1'
            if 'algorithm=' in otp_secret:
                algorithm = otp_secret.split('algorithm=')[1].split('&')[0]
                logging.info(f"Using algorithm: {algorithm}")
            
            period = 30
            if 'period=' in otp_secret:
                period_str = otp_secret.split('period=')[1].split('&')[0]
                period = int(period_str)
                logging.info(f"Using period: {period}")
            
            # Generate the OTP code
            if algorithm.upper() == 'SHA512':
                logging.info("Using custom TOTP implementation for SHA512")
                totp = CustomTOTP(secret_param, digits=digits, digest='sha512', interval=period)
            else:
                # Use standard PyOTP for other algorithms
                totp = pyotp.TOTP(secret_param, digits=digits, digest=algorithm.lower(), interval=period)
            
            otp_code = totp.now()
            logging.info(f"Generated OTP code: {otp_code}")
            return otp_code
        else:
            logging.info("Using direct secret")
            # Try to use the string directly as a secret
            totp = pyotp.TOTP(otp_secret)
            otp_code = totp.now()
            logging.info(f"Generated OTP code: {otp_code}")
            return otp_code
    except Exception as e:
        logging.error(f"Error generating OTP code: {str(e)}")
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Test OTP code generation')
    parser.add_argument('--otp-secret', help='OTP secret to use for code generation')
    args = parser.parse_args()
    
    # Use command-line argument if provided, otherwise use environment variable
    otp_secret = args.otp_secret
    if not otp_secret:
        load_dotenv()
        otp_secret = os.getenv("TELEKOM_OTP_SECRET")
    
    if otp_secret:
        logging.info(f"Loaded OTP secret with length: {len(otp_secret)}")
        otp_code = generate_otp_code(otp_secret)
        if otp_code:
            print(f"Generated OTP code: {otp_code}")
        else:
            print("Failed to generate OTP code")
    else:
        logging.error("No OTP secret provided")

if __name__ == "__main__":
    main()
