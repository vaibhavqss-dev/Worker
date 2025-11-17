import json
from databases.mongo import MongoDB 
from databases.postgres import PostgresDB
import sendemail.mail as mail
import databases.postgres as pg


pgDB = PostgresDB()
mn = MongoDB()

def patient_records(ch, method, properties, body):
    try:
        data = json.loads(body)
        mn.insert_mongodb(data["record"], "patient_records")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Message Processing Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

def consume_logs(ch, method, properties, body):
    try:
        data = json.loads(body)
        category = data['category']
        mn.insert_mongodb(data, category, "logs")
        #mail.send_mail(data)
        valid_counters = ["profile_viewed", "profile_updated", "records_viewed", "records_created"]
        if data['category'] in valid_counters:
            pgDB.counter_update_client_postgresql(data['category'], data['health_id'])
            pgDB.counter_update_healthcare_postgresql(data['category'], data['healthcare_id'])

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Message Processing Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

def consume_appointments(ch, method, properties, body):
    try:
        data = json.loads(body)
        # mn.insert_mongodb(data, "appointments", "db")
        pgDB.Insert_appointment_section(data)
        #mail.send_mail(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Message Processing Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

def appointment_update(ch, method, properties, body):
    try:
        data = json.loads(body)
        pgDB.Update_appointment_status(data["update"])
        #mail.send_mail(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Message Processing Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)








