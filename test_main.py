import unittest
from unittest.mock import patch, MagicMock
from main import fetch_random_users, update_winners_table, draw, main, connection, cursor


class TestLottery(unittest.TestCase):

    @patch('main.requests.get')
    def test_fetch_random_users(self, mock_get):
        mock_response = MagicMock()
        expected_json = [
            {
                "id": 1,
                "email": "a@a.com",
                "address": {"state": "CA"}
            },
            {
                "id": 2,
                "email": "b@b.com",
                "address": {"state": "TX"}
            }
        ]
        mock_response.json.return_value = expected_json
        mock_get.return_value = mock_response

        result = fetch_random_users(2)

        self.assertEqual(result, expected_json)
        mock_get.assert_called_once_with(
            "https://random-data-api.com/api/users/random_user?size=2")

    @patch('main.cursor')
    @patch('main.connection')
    def test_update_winners_table_insert(self, mock_connection, mock_cursor):
        user = {'id': 1, 'address': {'state': 'CA'},
                'email': 'a@a.com'}

        mock_cursor.fetchone.return_value = None

        update_winners_table(user)

        mock_cursor.execute.assert_called_with(
            "INSERT INTO winners (id, email, state) VALUES (?, ?, ?)",
            (user['id'], user['email'], user['address']['state'])
        )

        mock_connection.commit.assert_called_once()

    @patch('main.cursor')
    @patch('main.connection')
    def test_update_winners_table_update(self, mock_connection, mock_cursor):
        user = {'id': 2, 'address': {'state': 'CA'},
                'email': 'a@a.com'}

        mock_cursor.fetchone.return_value = (1, 'user1@example.com', 'CA')

        update_winners_table(user)

        mock_cursor.execute.assert_called_with(
            "UPDATE winners SET id = ?, email = ? WHERE state = ?",
            (user['id'], user['email'], user['address']['state'])
        )

        mock_connection.commit.assert_called_once()

    @patch('main.update_winners_table')
    @patch('main.fetch_random_users')
    @patch('main.cursor')
    @patch('main.time.sleep')
    def test_draw(self, mock_sleep, mock_cursor, mock_fetch_random_users, mock_update_winners_table):
        mock_cursor.fetchall.return_value = []

        mock_fetch_random_users.side_effect = [
            [
                {'id': 1, 'address': {'state': 'CA'},
                    'email': 'user1@example.com'},
                {'id': 2, 'address': {'state': 'NY'},
                    'email': 'user2@example.com'},
                {'id': 3, 'address': {'state': 'TX'},
                    'email': 'user3@example.com'},
                {'id': 4, 'address': {'state': 'FL'},
                    'email': 'user4@example.com'},
                {'id': 5, 'address': {'state': 'WA'}, 'email': 'user5@example.com'}
            ],
            [
                {'id': 6, 'address': {'state': 'NV'},
                    'email': 'user6@example.com'},
                {'id': 7, 'address': {'state': 'OR'},
                    'email': 'user7@example.com'},
                {'id': 8, 'address': {'state': 'AZ'},
                    'email': 'user8@example.com'},
                {'id': 9, 'address': {'state': 'CO'},
                    'email': 'user9@example.com'},
                {'id': 10, 'address': {'state': 'UT'},
                    'email': 'user10@example.com'}
            ]
        ]

        draw(5)

        mock_sleep.assert_called_once_with(10)

        mock_cursor.fetchall.assert_called()

        self.assertTrue(mock_fetch_random_users.called)

        self.assertEqual(mock_update_winners_table.call_count, 5)

        expected_calls = [
            (({'id': 1, 'address': {'state': 'CA'}, 'email': 'user1@example.com'},),),
            (({'id': 2, 'address': {'state': 'NY'}, 'email': 'user2@example.com'},),),
            (({'id': 3, 'address': {'state': 'TX'}, 'email': 'user3@example.com'},),),
            (({'id': 4, 'address': {'state': 'FL'}, 'email': 'user4@example.com'},),),
            (({'id': 5, 'address': {'state': 'WA'}, 'email': 'user5@example.com'},),)
        ]
        mock_update_winners_table.assert_has_calls(
            expected_calls, any_order=True)

    @patch('main.update_winners_table')
    @patch('main.fetch_random_users')
    @patch('main.cursor')
    def test_draw_duplication_not_in_the_final_batch(self, mock_cursor, mock_fetch_random_users, mock_update_winners_table):
        mock_cursor.fetchall.return_value = []

        mock_fetch_random_users.side_effect = [
            [
                {'id': 1, 'address': {'state': 'CA'},
                    'email': 'user1@example.com'},
                {'id': 2, 'address': {'state': 'NY'},
                    'email': 'user2@example.com'},
                {'id': 3, 'address': {'state': 'TX'},
                    'email': 'user3@example.com'},
                {'id': 4, 'address': {'state': 'FL'},
                    'email': 'user4@example.com'},
                {'id': 5, 'address': {'state': 'CA'}, 'email': 'user5@example.com'}
            ],
            [
                {'id': 6, 'address': {'state': 'NV'},
                    'email': 'user6@example.com'},
                {'id': 7, 'address': {'state': 'OR'},
                    'email': 'user7@example.com'},
                {'id': 8, 'address': {'state': 'AZ'},
                    'email': 'user8@example.com'},
                {'id': 9, 'address': {'state': 'CO'},
                    'email': 'user9@example.com'},
                {'id': 10, 'address': {'state': 'UT'},
                    'email': 'user10@example.com'}
            ]
        ]

        with patch('time.sleep', return_value=None):  # Skip the sleep
            draw(5)

        mock_cursor.fetchall.assert_called()

        self.assertTrue(mock_fetch_random_users.called)

        # pass ids = [1,2,3,4,5,6], 5 replaced 1
        self.assertEqual(mock_update_winners_table.call_count, 6)

        expected_calls = [
            (({'id': 5, 'address': {'state': 'CA'}, 'email': 'user5@example.com'},),),
            (({'id': 2, 'address': {'state': 'NY'}, 'email': 'user2@example.com'},),),
            (({'id': 3, 'address': {'state': 'TX'}, 'email': 'user3@example.com'},),),
            (({'id': 4, 'address': {'state': 'FL'}, 'email': 'user4@example.com'},),),
            (({'id': 6, 'address': {'state': 'NV'},
               'email': 'user6@example.com'},),)
        ]
        mock_update_winners_table.assert_has_calls(
            expected_calls, any_order=True)

    @patch('main.update_winners_table')
    @patch('main.fetch_random_users')
    @patch('main.cursor')
    # this ensure that we don't early exit the replacement behavior
    def test_draw_duplication_in_the_final_batch(self, mock_cursor, mock_fetch_random_users, mock_update_winners_table):
        mock_cursor.fetchall.return_value = []

        mock_fetch_random_users.side_effect = [
            [
                {'id': 1, 'address': {'state': 'CA'},
                    'email': 'user1@example.com'},
                {'id': 2, 'address': {'state': 'NY'},
                    'email': 'user2@example.com'},
                {'id': 3, 'address': {'state': 'TX'},
                    'email': 'user3@example.com'},
                {'id': 4, 'address': {'state': 'FL'},
                    'email': 'user4@example.com'},
                {'id': 5, 'address': {'state': 'CA'}, 'email': 'user5@example.com'}
            ],
            [
                {'id': 6, 'address': {'state': 'NV'},  # already filled the draw (5)
                    'email': 'user6@example.com'},
                {'id': 7, 'address': {'state': 'OR'},
                    'email': 'user7@example.com'},
                {'id': 8, 'address': {'state': 'CA'},  # still need to do the replacement
                    'email': 'user8@example.com'},
                {'id': 9, 'address': {'state': 'CO'},
                    'email': 'user9@example.com'},
                {'id': 10, 'address': {'state': 'UT'},
                    'email': 'user10@example.com'}
            ]
        ]

        with patch('time.sleep', return_value=None):  # Skip the sleep
            draw(5)

        mock_cursor.fetchall.assert_called()

        self.assertTrue(mock_fetch_random_users.called)

        # pass ids = [1,2,3,4,5,6,8], 5 replaced 1, 8 replaced 5
        self.assertEqual(mock_update_winners_table.call_count, 7)

        expected_calls = [
            (({'id': 8, 'address': {'state': 'CA'}, 'email': 'user8@example.com'},),),
            (({'id': 2, 'address': {'state': 'NY'}, 'email': 'user2@example.com'},),),
            (({'id': 3, 'address': {'state': 'TX'}, 'email': 'user3@example.com'},),),
            (({'id': 4, 'address': {'state': 'FL'}, 'email': 'user4@example.com'},),),
            (({'id': 6, 'address': {'state': 'NV'},
               'email': 'user6@example.com'},),)
        ]
        mock_update_winners_table.assert_has_calls(
            expected_calls, any_order=True)


if __name__ == '__main__':
    unittest.main()
