# Chess Rating Comparison
The notion that Lichess ratings are higher than Chess.com ratings is prevalent to the point of being a meme. This repository aims to provide a more scientific approach to this claim by comparing the ratings of players on both platforms.


## Dataset
In order to perform meaningful comparisons, we need a dataset that contains the ratings of players on both platforms. To do so, we take the naive approach of joining the two datasets on username.

To construct the dataset, we use the following steps:

1. Parse the Lichess games database to extract usernames and ratings.
2. Use the Chess.com API to extract Chess.com ratings for the same usernames.

Step 1 is done via a combination of `ratings.sh` and `postgres.py`.

`ratings.sh` parses a Lichess database file and extracts the usernames and ratings. It then writes the results to a CSV file.

`postgres.py` takes the CSV file and inserts the data into a PostgreSQL database.

Step 2 is done via `chesscom_updater.py`, which uses the Chess.com API to extract ratings for each username in the database. It then updates the database with the Chess.com ratings.

## TODO
- [ ] Wait for script to finish (ETA: 04/12/2025)
- [ ] Analysis
