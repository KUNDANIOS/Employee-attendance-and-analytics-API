1. Indexes. An index is a sorted lookup table, so MongoDB doesn’t read every document to find matches. I added one for each way the app searches:

Unique emp_code on employees: finds an employee fast, and stops two employees sharing a code.
Unique (emp_code, date) on attendance: this is the key one. It finds one person’s records for a month quickly, and it also guarantees one record per person per day.
(date, emp_code): the attendance list is sorted newest date first, and month reports scan a date range.
(status, date, emp_code): for the list when you filter by status.
(emp_code, punch_in): punch-out needs “the latest punch-in before this time”.
Employee indexes on department and joined_on: headcount queries.

I rejected an index on late_minutes: the leaderboard first narrows to one month by date and then adds up, so an index on late minutes would never be used.

2. Punch-in race. Suppose two requests for the same person arrive in the same millisecond. A naive app checks “is there a record yet?”, sees none for both, and inserts twice. My app skips the check and inserts directly. The unique (emp_code, date) index makes MongoDB accept the first insert and reject the second with a duplicate-key error. I catch that error and return 409. The database decides the winner, so there’s no timing gap to exploit. I tested this with 12 parallel requests: one 201, eleven 409s.

3. Ties. If two people have 60 late minutes each, they’re both rank 1, and the next person is rank 3 (1, 1, 3). A plain sorted list can’t show that, so I use MongoDB’s $rank. The limit comes after ranking and keeps rank <= limit. With limit=1 and a tie for first, you get both people. Cutting the list at one row before ranking would hide one of the tied people.

4. Headcount. “Present” and “late” can come from attendance records, but headcount is “how many employees work here”, and employees with no records still count. So the pipeline starts from the employees collection and attaches each person’s records. preserveNullAndEmptyArrays keeps someone even if they have none. Starting from the records would silently drop absent people and make attendance look better than it is. People who haven’t joined yet are left out by the joined_on filter.

5. 100x data. At 100k records the summary takes 0.74 s because it looks up each employee’s records every time. At 10 million, that grows. The fix is to keep running totals per employee per day or month and update them whenever a record changes, so reports read small totals instead of recounting raw records. Skipping to page 5000 with skip also gets slow, so you’d switch to “continue after this date” paging.