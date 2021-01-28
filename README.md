# mbd_project

page_id => page_name
- edit_per_day (0; +inf)
This number shows how many reviews one page has, in average, per day.
It is the number of reviews divided by the number of days passed since the first review of that page.

- recent_reviews [0, 1]
This is a percentage.
It is the number of "new" reviews divided by the total number of reviews.

- score [0, 3]
The score of the page
We define the score to be the average score of all reviews for a specified page. If a page has a score close to 3, it means that there are several reviews made a short time ago.
