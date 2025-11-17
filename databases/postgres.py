import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()

class PostgresDB:
    def __init__(self):
        self.POSTGRESQL_PORT=os.getenv("POSTGRESQL_PORT")
        self.POSTGRESQL_PASSWORD=os.getenv("POSTGRESQL_PASSWORD")
        self.POSTGRESQL_USER=os.getenv("POSTGRESQL_USER")
        self.POSTGRESQL_HOST=os.getenv("POSTGRESQL_HOST")
        self.POSTGRESQL_DB=os.getenv("POSTGRESQL_DB")

        self.connection = psycopg2.connect(
            dbname=self.POSTGRESQL_DB,
            user=self.POSTGRESQL_USER,
            password=self.POSTGRESQL_PASSWORD,
            host=self.POSTGRESQL_HOST,
            port=self.POSTGRESQL_PORT,
        )
        self.cursor = self.connection.cursor()

    def Insert_appointment_section(self, data):
        try:
            query = """
                INSERT INTO appointments (status, appointment_time, department, fullname, health_id, appointment_date, healthcare_id, note, healthcare_name, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                data["status"],
                data["appointment_time"],
                data["department"],
                data["fullname"],
                data["health_id"],
                data["appointment_date"],
                data["healthcare_id"],
                data["note"],
                data["healthcare_name"],
                data["created_at"],
                data["updated_at"]
            )
            self.cursor.execute(query, values)
            self.connection.commit()
        except Exception as e:
            print(f"PostgreSQL Error: {e}")
            self.connection.rollback()
        
    def Update_appointment_status(self, data):
        try:
            query = """
                UPDATE appointments
                SET status = %(status)s
                WHERE id = %(id)s 
                AND health_id = %(health_id)s 
                AND healthcare_id = %(healthcare_id)s;
            """
            values = {
                "status": data["status"],
                "id": data["id"],
                "health_id": data["health_id"],
                "healthcare_id": data["healthcare_id"]
            }
            self.cursor.execute(query, values)
            self.connection.commit()
        except Exception as e:
            print(f"PostgreSQL Error: {e}")
            self.connection.rollback()

    def counter_update_client_postgresql(self, category, health_id):
        try:        
            query = f"UPDATE client_stats SET {category} = {category} + 1 WHERE health_id = %s;"
            self.cursor.execute(query, (health_id,))
            self.connection.commit()
            print(f"{category} counter updated successfully for health_id {health_id}.")
        except Exception as e:
            print(f"PostgreSQL Error: {e}")
            self.connection.rollback()

    def counter_update_healthcare_postgresql(self, category, healthcare_id):
        try:        
            query = f"UPDATE healthcare_pref SET {category} = {category} + 1 WHERE healthcare_id = %s;"
            self.cursor.execute(query, (healthcare_id,))
            self.connection.commit()
            print(f"{category} counter updated successfully for healthcare_id {healthcare_id}.")
        except Exception as e:
            print(f"PostgreSQL Error: {e}")
            self.connection.rollback()