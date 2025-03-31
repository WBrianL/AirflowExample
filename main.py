from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import smtplib
from email.mime.text import MIMEText

def get_db_connection():
    return psycopg2.connect(
        dbname='tu_base_de_datos',
        user='tu_usuario',
        password='tu_contraseña',
        host='tu_host',
        port='tu_puerto'
    )

def send_email(to_email, subject, message):
    sender_email = "tu_correo@gmail.com"
    sender_password = "tu_contraseña"
    
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = to_email
    
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, msg.as_string())
        print(f"Correo enviado a {to_email}")
    except Exception as e:
        print(f"Error enviando correo a {to_email}: {e}")

def send_reminders():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT nombre, fecha_cita, hora_cita, correo FROM citas LIMIT 5")
    citas = cursor.fetchall()
    for cita in citas:
        subject = "Recordatorio de Cita"
        message = f"Hola {cita[0]}, te recordamos tu cita el {cita[1]} a las {cita[2]}."
        send_email(cita[3], subject, message)
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'send_cita_reminders',
    default_args=default_args,
    description='Envía recordatorios de citas por correo',
    schedule_interval='@daily',
)

reminder_task = PythonOperator(
    task_id='send_reminders_task',
    python_callable=send_reminders,
    dag=dag,
)
