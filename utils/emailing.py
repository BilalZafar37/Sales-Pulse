
# utils/emailing.py
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

def send_email(subject: str, sender: str, recipients: list[str], html_content: str,
               smtp_server: str, smtp_port: int, smtp_username: str, smtp_password: str):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = formataddr(("Sales Pulse", "podms@modern-electronics.com"))
    msg['To'] = ', '.join(recipients)

    msg.attach(MIMEText(html_content, 'html'))

    with smtplib.SMTP(smtp_server, smtp_port, timeout=20) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, recipients, msg.as_string())