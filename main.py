from typing import TypedDict, List
import requests
import sqlite3
import time
import logging


class Address(TypedDict):
    state: str


class User(TypedDict):
    id: int
    address: Address
    email: str


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

connection = sqlite3.connect('lottery.db')
cursor = connection.cursor()

# create the winners table if not exists
connection.execute('''
CREATE TABLE IF NOT EXISTS winners (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    state TEXT NOT NULL UNIQUE
)
''')
connection.commit()


def fetch_random_users(num: int) -> List[User]:
    url = f"https://random-data-api.com/api/users/random_user?size={num}"
    response = requests.get(url)
    return response.json()


def update_winners_table(user: User):
    state = user['address']['state']
    cursor.execute("SELECT * FROM winners WHERE state = ?", (state,))
    winner = cursor.fetchone()

    if winner:
        # Replace existing winner from the same state
        cursor.execute("UPDATE winners SET id = ?, email = ? WHERE state = ?",
                       (user['id'], user['email'], state))
        logging.info(
            f"Replaced winner from {state} with new user {user['email']}")
    else:
        # Insert new winner
        cursor.execute("INSERT INTO winners (id, email, state) VALUES (?, ?, ?)",
                       (user['id'], user['email'], state))
        logging.info(f"Added new winner from {state}: {user['email']}")

    connection.commit()


def print_winners():
    cursor.execute("SELECT * FROM winners")
    rows = cursor.fetchall()
    print(f"\n {len(rows)} Winners:")
    for row in rows:
        print(row)


def draw(amount: int) -> List[User]:
    cursor.execute("SELECT state FROM winners")
    rows = cursor.fetchall()
    winner_state_set = {row[0] for row in rows}

    while len(winner_state_set) < amount:
        users = fetch_random_users(5)
        for user in users:
            state = user['address']['state']
            if (len(winner_state_set) == amount and state in winner_state_set) or len(winner_state_set) < amount:
                update_winners_table(user)
                winner_state_set.add(user['address']['state'])

        time.sleep(10)


def main():
    draw(25)

    print_winners()

    connection.close()


if __name__ == "__main__":
    main()
