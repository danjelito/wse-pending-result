import pandas as pd
from pathlib import Path
import os


def load_single_pending_df(path_: str) -> pd.DataFrame:
    """Load pending result DF."""

    df = pd.read_excel(path_, skiprows=1, engine="xlrd")
    return df


def load_all_pending_dfs(path_: str) -> list[pd.DataFrame]:
    """Load all pending result DFs, return them as a list of DFs."""

    offline_classroom = load_single_pending_df(Path(path_, "Pending Results.xls"))
    offline_other = load_single_pending_df(Path(path_, "Pending Results (1).xls"))
    online_classroom = load_single_pending_df(Path(path_, "Pending Results (2).xls"))
    online_other = load_single_pending_df(Path(path_, "Pending Results (3).xls"))
    return [offline_classroom, offline_other, online_classroom, online_other]


def load_trainer_df(month: str) -> pd.DataFrame:
    """Load trainer DF which details the area of ET."""

    path = os.getenv("path_trainer_data")
    df_trainer = pd.read_excel(path, sheet_name=month)

    return df_trainer


def clean_trainer_name(df: pd.DataFrame, teacher_col: str) -> pd.Series:
    """Clean teacher's name."""

    teachers = (
        df[teacher_col]
        .str.title()
        .str.replace("\(.+\)", "", regex=True)
        .str.replace("\s+", " ", regex=True)
        .str.strip()
        .replace(
            {
                "Azhar Rahul": "Azhar Rahul Finaya",
                "Handayani Risma": "Handayani Khaerunisyah Risma",
                "Kartikasari Prettya": "Kartikasari Prettya Nur",
                "Ramadhan Ira Ragil": "Ramadhani Ira",
                "S Allan": "Santiago Allan",
                "Gandhama Jesita": "Ghandama Jesita",
                "Istiqomah Diah": "Toluhula Diah Istiqomah",
                "Putri Tiara": "Setiawan Tiara Putri",
                "Hamsah Ratnasari Handayani": "Hamsah Handayani Ratnasari",
            }
        )
    )
    return teachers


def clean_pending_df(dfs: list, date_exported: str, month: str) -> pd.DataFrame:
    """Clean pending dfs to obtain list of pending results per session."""

    df_clean = (
        # concat dfs that is obtained from load_all_pending_dfs
        pd.concat(dfs)
        # drop unused columns
        .drop(
            columns=["Level / Unit", "First Name", "Last Name", "Code", "Service Type"]
        )
        # drop duplicates based on this subset to get per session
        .drop_duplicates(
            subset=["Teacher", "Class Type", "Date", "Start Time"], keep="first"
        )
        # drop staff appointment
        .loc[lambda df_: df_["Class Type"] != "Staff Appointment"]
        # rename columns
        .rename(columns=lambda c: c.lower().replace("_", " "))
        # drop na rows and cols
        .dropna(subset=["teacher"])
        .dropna(how="all", axis=0)
        .dropna(how="all", axis=1)
        .assign(
            teacher=lambda df_: clean_trainer_name(df_, "teacher"),  # clean teacher name
            date=lambda df_: pd.to_datetime(df_["date"]),  # get the clean date
            date_exported=pd.to_datetime(date_exported),  # data exported date
        )
        # merge with et data to get area and position
        .merge(
            right=load_trainer_df(month),
            left_on="teacher",
            right_on="teacher",
            how="left",
        )
        # drop unused cols
        .drop(
            columns=[
                "teacher_working_days",
                "teacher_note_1",
                "teacher_note_2",
                "center name",
            ]
        )
        # sort columns
        .loc[
            :,
            [
                "teacher",
                "date",
                "start time",
                "class type",
                "teacher_position",
                "teacher_center",
                "teacher_area",
                "date_exported",
            ],
        ]
        # sort rows
        .sort_values(["teacher_area", "teacher_center", "teacher", "date"])
        .reset_index(drop=True)
    )

    return df_clean