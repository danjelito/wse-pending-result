def test_all_teacher_exist_in_coco_trainer_data(
    df_clean, teacher_col, teacher_area_col
):
    unmapped = df_clean.loc[df_clean[teacher_area_col].isna(), teacher_col].unique()
    assert (
        unmapped.shape[0] == 0
    ), f"some teacher are not listed in coco_trainer_data: {unmapped}"


def test_len_raw_eq_len_per_area(df_raw, df_area_list):
    assert len(df_raw) == sum(
        [len(df) for df in df_area_list]
    ), "Len DF raw != len DF area."
