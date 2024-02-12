import os
import module
import test
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

today = "2024-02-05"
month = "2024-01"  # note: used to retrieve sheet from coco trainer data
load_dotenv()


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
        .loc[lambda df_: np.sum(df_.iloc[:, -3:], axis=1) > 0]
        # clean col names and all
        .droplevel(0, axis=1)
        .rename(columns=lambda c: c.strftime("%b %Y"))
        .rename_axis(["Teacher Area", "Teacher"])
        .rename_axis([""], axis=1)
    )

if __name__ == "__main__":

    # load all pending dfs in a folder
    dfs = module.load_all_pending_dfs(Path("data", today))
    # clean data
    df_clean = module.clean_pending_df(dfs, today, month)
    # count pending result per month / create summary
    df_pending = count_pending_result(df_clean, today)

    # test
    test.test_all_teacher_exist_in_coco_trainer_data(df_clean)

    # create data per area
    df_clean = df_clean.rename(columns=lambda c: c.replace("_", " ").title())
    df_jkt1 = df_clean.loc[df_clean["Teacher Area"] == "JKT 1"]
    df_jkt2 = df_clean.loc[df_clean["Teacher Area"] == "JKT 2"]
    df_jkt3 = df_clean.loc[df_clean["Teacher Area"] == "JKT 3"]
    df_sby = df_clean.loc[df_clean["Teacher Area"] == "SBY"]
    df_bdg = df_clean.loc[df_clean["Teacher Area"] == "BDG"]
    df_onl = df_clean.loc[
        df_clean["Teacher Area"].isin(["Online", "Shared Account", "Ooolab"])
    ]
    df_oth = df_clean.loc[df_clean["Teacher Area"] == "Other"]

    # assert that no rows are missed
    assert len(df_clean) == sum(
        [len(df) for df in [df_jkt1, df_jkt2, df_jkt3, df_sby, df_bdg, df_onl, df_oth]]
    )

    # save df
    # write each dataframe to a different worksheet.
    filename = "output.xlsx"
    filepath = os.path.join("data", today, filename)
    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")

    df_pending.to_excel(writer, sheet_name="Summary", index=True)
    df_clean.to_excel(writer, sheet_name="All Area", index=False)
    df_jkt1.to_excel(writer, sheet_name="JKT 1", index=False)
    df_jkt2.to_excel(writer, sheet_name="JKT 2", index=False)
    df_jkt3.to_excel(writer, sheet_name="JKT 3", index=False)
    df_sby.to_excel(writer, sheet_name="SBY", index=False)
    df_bdg.to_excel(writer, sheet_name="BDG", index=False)
    df_onl.to_excel(writer, sheet_name="Online", index=False)
    df_oth.to_excel(writer, sheet_name="Other", index=False)
    print("file saved")
    writer.close()