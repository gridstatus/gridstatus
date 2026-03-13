from gridstatus.utils import make_availability_table, make_lmp_availability_table


def insert_readme(files, start_str, end_str, to_insert):
    import os

    for filepath in files:
        if not os.path.exists(filepath):
            print(f"Skipping {filepath}: file not found")
            continue
        with open(filepath, "r+") as f:
            content = f.read()
            if start_str not in content or end_str not in content:
                print(f"Skipping {filepath}: markers not found")
                continue
            insert_start = content.index(start_str) + len(start_str)
            insert_end = content.index(end_str)
            new_file_content = (
                content[:insert_start] + "\n" + to_insert + "\n" + content[insert_end:]
            )
            f.seek(0)
            f.write(new_file_content)
            f.truncate()


files = ["./docs/availability.md"]
insert_readme(
    files,
    "<!-- METHOD AVAILABILITY TABLE START -->",
    "<!-- METHOD AVAILABILITY TABLE END -->",
    make_availability_table(),
)

insert_readme(
    ["./docs/lmp.md"],
    "<!-- LMP AVAILABILITY TABLE START -->",
    "<!-- LMP AVAILABILITY TABLE END -->",
    make_lmp_availability_table(),
)
