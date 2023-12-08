import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine
import argparse
import json
from typing import Dict

connection = ps.connect(
        host = 'localhost',
        database = 'test',
        user = 'postgres',
        password = 'postgres',
        port = 5432
    )

def create_tables() -> None:
    cursor = connection.cursor()
    cursor.execute(open('../sql/schema.sql', 'r').read())
    connection.commit()
    cursor.close()

def execute_queries(save_format='json') -> None:
    cursor = connection.cursor()
    
    # Список комнат и количество студентов в каждой из них
    cursor.execute(open('../sql/first_query.sql', 'r').read())
    lst_first = cursor.fetchall()

    # 5 комнат, где самый маленький средний возраст студентов
    cursor.execute(open('../sql/second_query.sql', 'r').read())
    lst_second = cursor.fetchall()

    #5 комнат с самой большой разницей в возрасте студентов
    cursor.execute(open('../sql/third_query.sql', 'r').read())
    lst_third = cursor.fetchall()

    # Список комнат где живут разнополые студенты
    cursor.execute(open('../sql/fourth_query.sql', 'r').read())
    lst_fourth = cursor.fetchall()

    if save_format == 'json':
        save_in_json(data=lst_first, keys=['room', 'amount'], fileName='../../output_data/firstquery.json')
        save_in_json(data=lst_second, keys=['id', 'room'], fileName='../../output_data/secondquery.json')
        save_in_json(data=lst_third, keys=['id', 'room'], fileName='../../output_data/thirdquery.json')
        save_in_json(data=lst_fourth, keys=['id', 'room'], fileName='../../output_data/fourthquery.json')
    else:
        print('there is no such format of save file')

    for row in cursor.fetchall():
        print(row)
    
def save_in_json(data, keys, fileName) -> None:
    lst = [dict(zip(keys, values)) for values in data]
    json_object = json.dumps(lst, indent=len(keys))
    
    with open(fileName, 'w') as file:
        file.write(json_object)
    
def extract_data(students_path='../../data/students.json', rooms_path='../../data/rooms.json') -> Dict:
    students = None
    rooms = None
    with open(students_path, 'r') as file:
        students = json.load(file)
    with open(rooms_path, 'r') as file:
        rooms = json.load(file)
    return students, rooms

def transform_data(students, rooms) -> tuple:
    df_students = pd.json_normalize(students, meta=['birthday', 'id', 'name', 'room', 'sex'])
    df_students = df_students[['id', 'birthday', 'name',  'room', 'sex']]
    print('students dataframe')
    print(df_students.head(5))
    
    df_rooms = pd.json_normalize(rooms, meta=['id', 'name'])
    print('rooms dataframe')
    print(df_rooms.head(5))

    return df_students, df_rooms

def load_data(df_students, df_rooms) -> None:
    students_table = 'students'
    rooms_table = 'rooms'
    
    host = 'localhost'
    database = 'test'
    user = 'postgres'
    password = 'postgres'
    port = 5432

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    
    df_students.to_sql(students_table, engine, if_exists='append', index=False)
    df_rooms.to_sql(rooms_table, engine, if_exists='append', index=False)

    engine.dispose()
      

def main():
    parser = argparse.ArgumentParser(description='command interface')

    parser.add_argument('-s', '--students', action='store', help='provide path to students.json file')
    parser.add_argument('-r', '--rooms', action='store', help='provide path to rooms.json file')
    parser.add_argument('-f', '--format', action='store', help='provide example of saving files (json, xml)')
    
    args = parser.parse_args()

    create_tables()
    
    students, rooms = extract_data(args.students, args.rooms)
    df_students, df_rooms = transform_data(students, rooms)
    load_data(df_students, df_rooms)
    execute_queries(args.format)
    connection.close()


if __name__ == '__main__':
    main()
    