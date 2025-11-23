from flask_login import LoginManager

# Initialize the LoginManager
login_manager = LoginManager()
login_manager.login_view = "auth.login"