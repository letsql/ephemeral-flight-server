from collections import Counter

with open("draft") as infile:
    methods = Counter(line.strip().lower() for line in infile)
    for k, v in methods.most_common():
        print(k, v)

# TODO
# version 3
# list_catalogs 3
# list_databases 3
# create_database 3
# drop_database 3

# raw_sql 3

# DONE
# list_tables 3
# get_schema 3
# create_table 3
# do_connect 3
# drop_table 3
# drop_view 3