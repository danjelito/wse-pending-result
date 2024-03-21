import os
import re
from collections import namedtuple
from pathlib import Path

import pandas as pd


def load_single_pending_df(path: str) -> pd.DataFrame:
    """Load a single pending result DF.

    :param str path: Path to excel file.
    :return pd.DataFrame: DF.
    """
    df = pd.read_excel(path, skiprows=1, engine="xlrd")
    return df


def load_all_pending_dfs(dirpath: str) -> list[pd.DataFrame]:
    """Load all pending result DFs, return them as one single DF.

    :param str dirpath: Path to folder that contains the excel files.
    :return pd.DataFrame: Concatted DF.
    """
    filepaths = [f for f in os.listdir(dirpath) if re.match("Pending", f)]
    dfs = [load_single_pending_df(Path(dirpath, filepath)) for filepath in filepaths]
    return pd.concat(dfs)


def load_trainer_df(month: str) -> pd.DataFrame:
    """Load trainer DF which details the area of ET.

    :param str month: Current month in format YYYY-MM.
    :return pd.DataFrame: Trainer DF for the current month.
    """
    path = os.getenv("path_trainer_data")
    df_trainer = pd.read_excel(path, sheet_name=month)
    return df_trainer


def clean_trainer_name(df: pd.DataFrame, teacher_col: str) -> pd.Series:
    """Clean teacher name in DF column.

    :param pd.DataFrame df: Pending result DF.
    :param str teacher_col: Name of the teacher column.
    :return pd.Series: Clean teacher name.
    """
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


def clean_pending_df(df: pd.DataFrame, date_exported: str, month: str) -> pd.DataFrame:
    """Clean pending result DF.

    :param pd.DataFrame df: Raw pending result DF.
    :param str date_exported: Date of exported, to create column of exported date.
    :param str month: Current month, for arg to load_trainer_df function.
    :return pd.DataFrame: Clean pending result DF.
    """

    col_to_be_dropped_1 = ["Level / Unit", "First Name", "Last Name", "Code", "Service Type",]
    col_to_be_dropped_2 = ["teacher_working_days","teacher_note_1","teacher_note_2","center name",]
    col_duplicate_subset = ["Teacher", "Class Type", "Date", "Start Time"]
    col_order = [
        "teacher", "date", "start time", "class type", "teacher_position", 
        "teacher_center", "teacher_area", "date_exported",
    ]
    col_for_sorting = ["teacher_area", "teacher_center", "teacher", "date"]
    # class to manually exclude from the data and report because already closed
    classes_to_exclude = [
        "Jurado Michael John 04 Feb 2024 16:00 Complementary Class",
    ]
    
    df_clean = (
        df
        # drop unused columns
        .drop(columns=col_to_be_dropped_1)
        # drop duplicates based on this subset to get per session
        .drop_duplicates(subset=col_duplicate_subset, keep="first")
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
            right_on="coco_teacher_name",
            how="left",
        )
        # drop unused cols again
        .drop(columns=col_to_be_dropped_2)
        # sort columns
        .loc[:,col_order]
        # sort rows
        .sort_values(col_for_sorting)
        .reset_index(drop=True)
        .rename(columns=lambda c: c.replace("_", " ").title())
    )
    # ! manually exlcude class
    # sometimes there are classes that have been closed by still appears
    def _make_class_identifier(df_clean):
        """A functio to make class identifier to manually exclude class."""
        classes = (
            df_clean["Teacher"] + " " 
            + df_clean["Date"].dt.strftime("%d %b %Y") + " " 
            + df_clean["Start Time"] + " " 
            + df_clean["Class Type"]
        )
        return classes
    df_clean = df_clean.loc[
        lambda df_: ~(_make_class_identifier(df_).isin(classes_to_exclude))
    ]
    return df_clean


def count_pending_result(df: pd.DataFrame, today: str, excluded_et: list = None) -> pd.DataFrame:
    """Get summary of pending result in the last 365 days,
    grouped by area and teacher, pivoted per month.

    :param pd.DataFrame df: Clean pending result DF from clean_pending_df function.
    :param str today: Today's date.
    :param list excluded_et: ETs to be exclued from the summary.
    :return pd.DataFrame: Pivoted pending result DF by area, teacher and month.
    """
    if excluded_et:
        df = (df
            .rename(columns=lambda c: c.replace(" ", "_").lower())
            .loc[lambda df_: ~(df_["teacher"].isin(excluded_et))]
        )

    return (
        df
        .rename(columns=lambda c: c.replace(" ", "_").lower())
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


def create_pending_df_per_area(df_clean: pd.DataFrame) -> namedtuple:
    """Create pending DF per area.

    :param pd.DataFrame df_clean: Clean pending result df from clean_pending_df function.
    :return namedtuple: namedtuple for each area DF.
    """
    df_jkt1 = df_clean.loc[df_clean["Teacher Area"] == "JKT 1"]
    df_jkt2 = df_clean.loc[df_clean["Teacher Area"] == "JKT 2"]
    df_jkt3 = df_clean.loc[df_clean["Teacher Area"] == "JKT 3"]
    df_sby = df_clean.loc[df_clean["Teacher Area"] == "SBY"]
    df_bdg = df_clean.loc[df_clean["Teacher Area"] == "BDG"]
    df_onl = df_clean.loc[
        df_clean["Teacher Area"].isin(["Online", "Shared Account", "Ooolab"])
    ]
    df_oth = df_clean.loc[df_clean["Teacher Area"] == "Other"]
    AreaDF = namedtuple(
        "AreaDF", ["jkt_1", "jkt_2", "jkt_3", "sby", "bdg", "onl", "oth"]
    )
    return AreaDF(df_jkt1, df_jkt2, df_jkt3, df_sby, df_bdg, df_onl, df_oth)


def create_folder_if_not_exist(folder_path: str):
    """
    Create the specified folder if it does not exist.

    :param str folder_path: Path of the folder to be created.
    """
    folder_path = Path(folder_path)

    if not folder_path.is_dir():
        folder_path.mkdir(parents=True, exist_ok=True)


def save_multiple_dfs(df_dict: dict, filepath: str):
    """
    Save multiple DataFrames to an Excel file.

    :param dict df_dict: A dictionary where keys are sheet names and values are DataFrames.
    :param str filepath: Path to the Excel file.
    """
    filepath = Path(filepath)
    parent = filepath.parent
    create_folder_if_not_exist(parent)

    if filepath.exists():
        raise FileExistsError("The file already exists.")

    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")

    for sheet_name, df in df_dict.items():
        df.to_excel(writer, sheet_name=sheet_name, index=True)

    writer.close()