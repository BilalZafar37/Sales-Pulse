from werkzeug.security import generate_password_hash
hashed = generate_password_hash("Admin@123!")  # store in SP_Users.Password
print(hashed)
