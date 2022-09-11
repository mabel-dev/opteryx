
- The conventions in SQL Engines generally make good sense, follow them
- Have real systems and real users as your beta testers
- Unit testing is fine, but write hundreds of tests cases which run real SQL queries
- You can't fabricate test data for all your test scenarios
- Storage read speed will kill any performance boosts from algorithmic improvements
- If you don't control the writing of the data - assume the worst


PyArrow is awesome, but it has bugs, odd limitations and some parts are so slow it hurts.

bugs
- date diff just doesn't work for dates

off limitations
- can't join on tables with arrays or structs

so slow it hurts
- the file system abstractions are 4x slower than the next slowest ways to access S3 I've tried