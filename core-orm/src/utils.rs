use chrono::{NaiveDate, Duration};

pub fn date_range(start_date: NaiveDate, end_date: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = vec![];
    let mut curr_date = start_date;
    while curr_date < end_date {
        dates.push(curr_date);
        curr_date += Duration::days(1);
    }

    return dates;
}