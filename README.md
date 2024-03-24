# Pending Result Report Generator

## Data Source
- Pending results from Core Course, both from offline and online centers. There are four files exported.

## External Dependencies
1. File `Trainer Working Days (Sync with Local).xlsx`, located on Devan's Google Drive. This is used to get each teacher's center. Update it on a monthly basis. Download the file from Google Drive and place it into your local folder.

## How to Use
- Download the pending result report from Core Course.
- Place the files into the `data/{date}` folder, where `{date}` is the current date.
- Create a `.env` file in the project's root directory and add the link to your local `Trainer Working Days (Sync with Local).xlsx` file with the variable name `path_trainer_data`.
- In `main.py`, change the `today` variable to the current date to fetch the pending result files.
- Also in `main.py`, change the `month` variable to get the correct sheet in the `Trainer Working Days (Sync with Local).xlsx` file.
- Run `main.py`.