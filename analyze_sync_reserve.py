import gridstatus

iso = gridstatus.PJM()
df = iso.get_sync_reserve_events(verbose=True)

print(f"Total events: {len(df)}")
print(f"\nColumns: {df.columns.tolist()}")
print("\nSample data:")
print(df.head(10))

print(f"\n{'=' * 80}")
print("PRIMARY KEY ANALYSIS")
print(f"{'=' * 80}")

# Test different primary key combinations
key_combos = [
    ["Interval Start"],
    ["Interval Start", "Interval End"],
    ["Interval Start", "Synchronized Subzone"],
    ["Interval Start", "Synchronized Reserve Zone"],
    ["Interval Start", "Interval End", "Synchronized Subzone"],
    ["Interval Start", "Interval End", "Synchronized Reserve Zone"],
    [
        "Interval Start",
        "Interval End",
        "Synchronized Reserve Zone",
        "Synchronized Subzone",
    ],
]

for keys in key_combos:
    dups = df.duplicated(subset=keys, keep=False)
    num_dups = dups.sum()
    print(f"\nKeys: {keys}")
    print(f"  Duplicates: {num_dups}")
    if num_dups > 0:
        print("  Sample duplicates:")
        print(
            df[dups]
            .sort_values(keys)
            .head(10)[keys + ["Duration", "Percent Deployed"]],
        )

print(f"\n{'=' * 80}")
print("NULL VALUE ANALYSIS")
print(f"{'=' * 80}")
for col in df.columns:
    null_count = df[col].isnull().sum()
    if null_count > 0:
        print(f"{col}: {null_count} nulls ({100 * null_count / len(df):.1f}%)")
