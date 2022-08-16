def check_lmp_columns(df):
    assert set(df.columns) == set(
        ["Time", "Market", "Node", "Node Type", "LMP", "Energy", "Congestion", "Loss"],
    )
