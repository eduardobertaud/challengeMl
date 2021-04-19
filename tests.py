import unittest
import json
from app import app


class BasicTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        app.config['DEBUG'] = False
        self.app = app.test_client()

    def tearDown(self):
        pass

    # Check if response is 200
    def test_root(self):
        response = self.app.get('/users/1', follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def test_json(self):
        response = self.app.get('/users/1')
        json_data = json.loads(response.data)

        user = json_data['1']
        self.assertEqual(user['avg_ratings_previous'], 3.5)
        self.assertEqual(user['nb_previous_ratings'], 1.0)
        self.assertEqual(user['userId'], 1.0)

    # Check if response is 400
    def test_bad_root(self):
        response = self.app.get('/users/bad_data', follow_redirects=True)
        self.assertEqual(response.status_code, 400)

    # Validate if content returned is JSON
    def test_index_content(self):
        response = self.app.get('/users/1', follow_redirects=True)
        self.assertEqual(response.content_type, "application/json")

    # Check for data returned
    def test_index_data(self):
        response = self.app.get('/users/1', follow_redirects=True)
        self.assertTrue(b'avg_ratings_previous' in response.data)


if __name__ == "__main__":
    unittest.main()
