import isodata

start_str = "<!-- METHOD AVAILABILITY TABLE START -->"
end_str = "<!-- METHOD AVAILABILITY TABLE END -->"

with open("README.md", "r+") as f:
    content = f.read()
    start = content.index(start_str) + len(start_str)
    end = content.index(end_str)
    new_content = (
        content[:start]
        + "\n"
        + isodata.make_availability_table()
        + "\n"
        + content[end:]
    )

    f.seek(0)
    f.write(new_content)
    f.truncate()
