import test
from collections import namedtuple
from pathlib import Path

from dotenv import load_dotenv

import module

today = "2024-03-18"  # note: used to get the folder containing pending result
month = "2024-02"  # note: used to retrieve sheet from coco trainer data
load_dotenv()  # load secret variable
# et to be exlcuded from the summary because already resigned
excluded_et = [
    "Jane Quinn Madeline",
    "Priscilla Yokhebed"
    "Rifani Aurora Nurhidayah",
    "Nasarah Nadya",
    "Louei Frentzen Caesar",
    "Bushey James Michael",
    "Mowatt Peter Denis",
    "Azhar Rahul Finaya",
    "Kartikasari Prettya Nur",
    "Laurendeau Derek",
]


if __name__ == "__main__":
    # load all pending dfs in a folder
    df = module.load_all_pending_dfs(Path("data", today))
    # clean data
    df_clean = module.clean_pending_df(df, today, month)
    # create summary
    df_pending = module.count_pending_result(df_clean, today, excluded_et)
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
    filepath = Path("data", today, "output.xlsx")
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
    module.save_multiple_dfs(df_dict=to_save, filepath=filepath)
    print("file saved")
    