from cryptography.fernet import Fernet

# Gera uma chave para criptografia
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# Sua senha do banco de dados
password = b"<coloque aqui sua senha>"
encrypted_password = cipher_suite.encrypt(password)

# Salva a chave e a senha criptografada em um arquivo
with open("config.py", "w") as config_file:
    config_file.write(f'KEY = "{key.decode()}"\n')
    config_file.write(f'ENCRYPTED_PASSWORD = "{encrypted_password.decode()}"\n')

print("Senha criptografada e armazenada em config.py.")
