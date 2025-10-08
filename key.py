# from cryptography.fernet import Fernet
# print(Fernet.generate_key().decode())

import secrets
print(secrets.token_hex(16))  # Outputs a 32-char hex string, e.g., 'a1b2c3d4e5f67890abcdef1234567890'