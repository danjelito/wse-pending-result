import os
import module
import test
from pathlib import Path
from collections import namedtuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv


today = "2024-03-18"  # note: used to get the folder containing pending result
month = "2024-02"  # note: used to retrieve sheet from coco trainer data
load_dotenv()  # load secret variable


if __name__ == "__main__":

    # load all pending dfs in a folder
    df = module.load_all_pending_dfs(Path("data", today))
    # clean data
    df_clean = module.clean_pending_df(df, today, month)
    # create summary
    df_pending = module.count_pending_result(df_clean, today)
    # create DF per area
    area_df = module.create_pending_df_per_area(df_clean)

    # test
    test.test_all_teacher_exist_in_coco_trainer_data(df_clean, "Teacher", "Teacher Area")
    df_area_list = [
        area_df.jkt_1, area_df.jkt_2, area_df.jkt_3,
        area_df.sby, area_df.bdg, area_df.onl, area_df.oth
    ]
    test.test_len_raw_eq_len_per_area(df_clean, df_area_list)

    # save df
    # write each dataframe to a different worksheet.
    filename = "output.xlsx"
    filepath = os.path.join("data", today, filename)

    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")
    to_save = {
        "Summary": df_pending,
        "All Area": df_clean,
        "JKT 1": area_df.jkt_1,
        "JKT 2": area_df.jkt_2,
        "JKT 3": area_df.jkt_3,
        "SBY": area_df.sby,
        "BDG": area_df.bdg,
        "Online": area_df.onl,
        "Other": area_df.oth,
    }
    for sheet_name, df in to_save.items():
        df.to_excel(writer, sheet_name=sheet_name, index=True)
    writer.close()
    print("file saved")
