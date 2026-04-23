import pandas as pd
import pyodbc

# читаем CSV
df = pd.read_csv(r"C:\OLAP_DATA\DimPatients.csv", sep=';', encoding='cp1251')

# подключение к SQL Server
conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-JC3PKHA;"
    "Database=DentalClinicDB;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO DimPatients (PatientID, Name, Age, Phone)
        VALUES (?, ?, ?, ?)
    """,
    row['id'],
    row['name'],
    row['age'],
    row['phone']
    )

conn.commit()
cursor.close()
conn.close()

print("Готово! Данные загружены")