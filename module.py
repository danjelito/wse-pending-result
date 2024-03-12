import os
import re
from pathlib import Path

import numpy as np
import pandas as pd


def load_single_pending_df(path: str) -> pd.DataFrame:
    """Load pending result DF."""

    df = pd.read_excel(path, skiprows=1, engine="xlrd")
    return df


def load_all_pending_dfs(dirpath: str) -> list[pd.DataFrame]:
    """Load all pending result DFs, return them as a list of DFs."""

    filepaths = [f for f in os.listdir(dirpath) if re.match("Pending", f)]
    dfs = [load_single_pending_df(Path(dirpath, filepath)) for filepath in filepaths]
    return dfs


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
            teacher=lambda df_: clean_trainer_name(
                df_, "teacher"
            ),  # clean teacher name
            date=lambda df_: pd.to_datetime(df_["date"]),  # get the clean date
            date_exported=pd.to_datetime(date_exported),  # data exported date
        )
        # merge with et data to get area and position
        .merge(
            right=load_trainer_df(month),
            left_on="teacher",
            right_on="coco_teacher_name",
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


def count_pending_result(df: pd.DataFrame, today: str) -> pd.DataFrame:
    """Get summary of pending result in the last 365 days,
    grouped by area and teacher, pivoted per month.
    """

    return (
        df
        # filter only pending result for the past 365 days
        .loc[lambda df_: (pd.to_datetime(today) - df_["date"]).dt.days <= 365]
        # group
        .groupby(["teacher_area", "teacher", pd.Grouper(key="date", freq="1M")])
        .agg(num_session_with_pending_res=("teacher", "count"))
        .reset_index()
        # pivot
        .pivot(index=["teacher_area", "teacher"], columns="date")
        .sort_index(axis="columns", ascending=False)
        # fill na with 0
        .fillna(0)
        # note: do not display trainer if the last 3 months pending results is 0
        # clean col names and all
        .droplevel(0, axis=1)
        # .sort_index(axis="columns", ascending=False)
        .rename(columns=lambda c: c.strftime("%b %Y"))
        .rename_axis(["Teacher Area", "Teacher"])
        .rename_axis([""], axis=1)
    )
