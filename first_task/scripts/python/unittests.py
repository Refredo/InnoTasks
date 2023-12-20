import unittest
import json
import pandas as pd
import psycopg2 as ps
import os
from unittest.mock import patch, MagicMock
from script import (
    create_tables,
    create_indexes,
    execute_queries,
    save_in_json,
    extract_data,
    transform_data,
    load_data,
)

class Unittests(unittest.TestCase):

    def setUp(self):
        self.test_students_path = '../../data/students.json'
        self.test_rooms_path = '../../data/rooms.json'
        self.test_format = 'json'

        self.test_connection = ps.connect(
            host='localhost',
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=5432
        )
        
        create_tables(self.test_connection)
        create_indexes(self.test_connection)

    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data=json.dumps({'id': 1, 'name': 'Test Room'}))
    def test_extract_data(self, mock_open):
        students, rooms = extract_data(self.test_students_path, self.test_rooms_path)

        self.assertEqual(students, {'id': 1, 'name': 'Test Room'})
        self.assertEqual(rooms, {'id': 1, 'name': 'Test Room'})

    def test_transform_data(self):
        students_data = [{'id': 1, 'name': 'John', 'birthday': '1990-01-01', 'room': 101, 'sex': 'M'}]
        rooms_data = [{'id': 101, 'name': 'Room 101'}]

        df_students, df_rooms = transform_data(students_data, rooms_data)

        self.assertIsInstance(df_students, pd.DataFrame)
        self.assertIsInstance(df_rooms, pd.DataFrame)

    def test_load_data(self):
        df_students = pd.DataFrame({'id': [1], 'birthday': ['1990-01-01'], 'name': ['John'], 'room': [101], 'sex': ['M']})
        df_rooms = pd.DataFrame({'id': [101], 'name': ['Room 101']})

        engine_mock = MagicMock()
        with patch('script.create_engine', return_value=engine_mock):
            load_data(df_students, df_rooms)

        engine_mock.dispose.assert_called_once()

    def tearDown(self):
        self.test_connection.close()

    def test_execute_queries(self):
        execute_queries(self.test_connection, save_format='json')
        
        self.assertTrue(os.path.exists('../../output_data/firstquery.json'))
        self.assertTrue(os.path.exists('../../output_data/secondquery.json'))
        self.assertTrue(os.path.exists('../../output_data/thirdquery.json'))
        self.assertTrue(os.path.exists('../../output_data/fourthquery.json'))

    def test_save_in_json(self):
        data = [(1, 'room1'), (2, 'room2')]
        keys = ['id', 'room']
        file_name = '../../output_data/test_output.json'

        save_in_json(data, keys, file_name)

        with open(file_name, 'r') as file:
            json_content = file.read()
            self.assertIn('"id": 1', json_content)
            self.assertIn('"room": "room1"', json_content)

    def test_create_tables(self):
        with self.assertLogs(level='INFO') as log_output:
            create_tables(self.test_connection)

        self.assertIn('Tables were created successfully', log_output.output[0])

    def test_create_indexes(self):
        with self.assertLogs(level='INFO') as log_output:
            create_indexes(self.test_connection)

        self.assertIn('Indexes were created successfully', log_output.output[0])


if __name__ == '__main__':
    unittest.main()
