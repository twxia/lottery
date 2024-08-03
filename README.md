# Lottery

## Problem

Company X wants to reward some of its clients for their continued support and wants to create a lottery system to do so. They have an API they can query to get a list of random clients from which to choose. They want to poll their API until they get 25 winners from different states in the United States, 1 winner per state.

## Exercise

Write a Python script that will:

- Run every 10 seconds until all 25 winners are found
- Query data using this exact endpoint and parameters: `https://random-data-api.com/api/users/random_user?size=5`
- Insert this data into a “winner” table in a local database (any type of DB will do)
  - The table will have the “id”, “email” and “state” of the client
  - If there is already a winner from that state, replace it with the new one, otherwise create a new winner
- Print some logs to show changes in the table
- Print the table when done (the table should have 25 entries at the end of the lottery)

## Prerequisites

- Python version is 3.10 or later

## Commands

```
> python3.10 -m unittest test_main.py # test

> python3.10 main.py # run the program
```
