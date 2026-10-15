each row follows the same pattern: what’s wrong, what input shows it, and how it’s fixed. The easiest to demonstrate are:

#10 page skip: page * page_size skips the first page, so page 1 shows employees 21 to 40.
#4 rounding: round(8.125, 2) gives 8.12 in Python, but the rule says 8.13.
#16 loading everything: the list loads all records into memory, then sorts and slices in Python.