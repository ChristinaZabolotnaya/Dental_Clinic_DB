import pandas as pd
import pyodbc
from typing import Any


def connect_db() -> pyodbc.Connection:
    """
    Создает подключение к базе данных SQL Server.

    Returns:
        pyodbc.Connection: объект подключения
    """
    return pyodbc.connect(
        "Driver={SQL Server};"
        "Server=DESKTOP-JC3PKHA;"
        "Database=DentalClinicDB;"
        "Trusted_Connection=yes;"
    )


def clear_tables(cursor: pyodbc.Cursor) -> None:
    """
    Очищает таблицы перед загрузкой данных.

    Args:
        cursor (pyodbc.Cursor): курсор базы данных
    """
    tables = [
        "FactVisits",
        "DimDoctors",
        "DimVisitType",
        "DimDate",
        "DimDiagnoses"
    ]

    for table in tables:
        cursor.execute(f"DELETE FROM {table}")

    print("Таблицы очищены")


def load_csv(file_path: str, table_name: str, cursor: pyodbc.Cursor, conn: pyodbc.Connection) -> None:
    """
    Загружает данные из CSV файла в таблицу (Dimension).

    Args:
        file_path (str): путь к CSV файлу
        table_name (str): имя таблицы
        cursor (pyodbc.Cursor): курсор БД
        conn (pyodbc.Connection): подключение к БД
    """
    df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')

    for _, row in df.iterrows():
        values: tuple[Any, ...] = tuple(row)
        placeholders = ",".join(["?"] * len(values))

        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(sql, values)

    conn.commit()
    print(f"{table_name} загружена")


def safe_float(value: Any) -> float:
    """
    Безопасно преобразует значение в float (учитывает запятую).

    Args:
        value (Any): входное значение

    Returns:
        float: преобразованное число
    """
    return float(str(value).replace(',', '.'))


def load_fact(file_path: str, cursor: pyodbc.Cursor, conn: pyodbc.Connection) -> None:
    """
    Загружает данные в таблицу фактов FactVisits.

    Args:
        file_path (str): путь к CSV файлу
        cursor (pyodbc.Cursor): курсор БД
        conn (pyodbc.Connection): подключение к БД
    """
    df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO FactVisits (
                FactID, DateID, PatientID, DoctorID, DiagnosisID, VisitTypeID,
                ServiceCost, VisitCount, Profit, PatientCount, RepeatRate, AvgTreatmentCost
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        int(row['FactID']),
        int(row['DateID']),
        int(row['PatientID']),
        int(row['DoctorID']),
        int(row['DiagnosisID']),
        int(row['VisitTypeID']),
        safe_float(row['ServiceCost']),
        int(row['VisitCount']),
        safe_float(row['Profit']),
        int(row['PatientCount']),
        safe_float(row['RepeatRate']),
        safe_float(row['AvgTreatmentCost'])
        )

    conn.commit()
    print("FactVisits загружена")


def main() -> None:
    """
    Основная функция запуска ETL-процесса.
    """
    try:
        conn = connect_db()
        cursor = conn.cursor()

        # Очистка таблиц
        clear_tables(cursor)
        conn.commit()

        # Загрузка измерений
        load_csv(r"C:\OLAP_DATA\DimDoctors.csv", "DimDoctors", cursor, conn)
        load_csv(r"C:\OLAP_DATA\DimVisitType.csv", "DimVisitType", cursor, conn)
        load_csv(r"C:\OLAP_DATA\DimDate.csv", "DimDate", cursor, conn)
        load_csv(r"C:\OLAP_DATA\DimDiagnoses.csv", "DimDiagnoses", cursor, conn)

        # Загрузка фактов
        load_fact(r"C:\OLAP_DATA\FactVisits.csv", cursor, conn)

        print(" ВСЕ ТАБЛИЦЫ ЗАГРУЖЕНЫ УСПЕШНО")

    except Exception as e:
        print(f" Ошибка: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
