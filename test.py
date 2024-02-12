def test_all_teacher_exist_in_coco_trainer_data(df_clean):
    unmapped = df_clean.loc[df_clean["teacher_area"].isna(), "teacher"].unique()
    assert (
        unmapped.shape[0] == 0
    ), f"some teacher are not listed in coco_trainer_data: {unmapped}"