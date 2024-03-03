import os
import module
import test
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv


today = "2024-03-03" # note: used to get the folder containing pending result
month = "2024-01"  # note: used to retrieve sheet from coco trainer data
load_dotenv() # load secret variable


if __name__ == "__main__":

    # load all pending dfs in a folder
    dfs = module.load_all_pending_dfs(Path("data", today))
    # clean data
    df_clean = module.clean_pending_df(dfs, today, month)
    # count pending result per month / create summary
    df_pending = module.count_pending_result(df_clean, today)

    # test
    test.test_all_teacher_exist_in_coco_trainer_data(df_clean)

    # create data per area
    df_clean = df_clean.rename(columns=lambda c: c.replace("_", " ").title())
    df_jkt1 = df_clean.loc[df_clean["Teacher Area"] == "JKT 1"]
    df_jkt2 = df_clean.loc[df_clean["Teacher Area"] == "JKT 2"]
    df_jkt3 = df_clean.loc[df_clean["Teacher Area"] == "JKT 3"]
    df_sby = df_clean.loc[df_clean["Teacher Area"] == "SBY"]
    df_bdg = df_clean.loc[df_clean["Teacher Area"] == "BDG"]
    df_onl = df_clean.loc[df_clean["Teacher Area"].isin(["Online", "Shared Account", "Ooolab"])]
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
    to_save = {
        "Summary": df_pending,
        "All Area": df_clean,
        "JKT 1": df_jkt1,
        "JKT 2": df_jkt2,
        "JKT 3": df_jkt3,
        "SBY": df_sby,
        "BDG": df_bdg,
        "Online": df_onl,
        "Other": df_oth,
    }
    for sheet_name, df in to_save.items():
        df.to_excel(writer, sheet_name=sheet_name, index=True)
    writer.close()
    print("file saved")