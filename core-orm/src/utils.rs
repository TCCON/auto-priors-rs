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

/// Return `true` if two (possibly open ended) date ranges overlap
/// 
/// Note that this assumes that the end date is exclusive, thus if
/// one ranges end date is equal to the other's start date, the
/// result is `false`:
/// 
/// ```
/// use chrono::NaiveDate;
/// use tccon_priors_orm::utils::date_ranges_overlap;
/// 
/// let d1 = NaiveDate::from_ymd(2010, 1, 1);
/// let d2 = NaiveDate::from_ymd(2010, 2, 1);
/// 
/// assert_eq!(date_ranges_overlap(d1, Some(d2), d2, None), false);
/// ```
/// 
/// # Parameters
/// * `r1_start`, `r2_start` - first date in each range.
/// * `r1_end`, `r2_end` - last date (exclusive) in each range. If the range is
///   open-ended, pass `None`.
/// 
/// # Returns
/// `true` if the ranges overlap by at least 1 day, `false` otherwise.
pub fn date_ranges_overlap(r1_start: NaiveDate, r1_end: Option<NaiveDate>, r2_start: NaiveDate, r2_end: Option<NaiveDate>) -> bool {
    if let Some(r1_end) = r1_end {
        if r2_start >= r1_end { return false; }   
    }
    if let Some(r2_end) = r2_end {
        if r2_end <= r1_start { return false; }
    }
    return true
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::utils::date_ranges_overlap;

    #[test]
    fn test_date_range_overlap() -> anyhow::Result<()> {
        let r1_start = NaiveDate::from_ymd(2010, 1, 1);
        let r1_end = NaiveDate::from_ymd(2010, 1, 31);
        let r2_before = NaiveDate::from_ymd(2009, 12, 1);
        let r2_before2 = NaiveDate::from_ymd(2009, 12, 15);
        let r2_between = NaiveDate::from_ymd(2010, 1, 15);
        let r2_after = NaiveDate::from_ymd(2010, 2, 15);
        let r2_after2 = NaiveDate::from_ymd(2010, 3, 1);
        
        // Test when both ranges are open ended, making sure that the result is symmetrical
        assert_eq!(date_ranges_overlap(r1_start, None, r2_before, None), true);
        assert_eq!(date_ranges_overlap(r1_start, None, r2_between, None), true);
        assert_eq!(date_ranges_overlap(r1_start, None, r2_after, None), true);

        assert_eq!(date_ranges_overlap(r2_before, None, r1_start, None), true);
        assert_eq!(date_ranges_overlap(r2_between, None, r1_start, None), true);
        assert_eq!(date_ranges_overlap(r2_after, None, r1_start, None), true);
        
        // Test when one range has an end date - the only non-overlapping cases should be
        // when the start date of the open ended range is after the end date of the closed
        // range.
        assert_eq!(date_ranges_overlap(r1_start, Some(r1_end), r2_before, None), true);
        assert_eq!(date_ranges_overlap(r1_start, Some(r1_end), r2_between, None), true);
        assert_eq!(date_ranges_overlap(r1_start, Some(r1_end), r2_after, None), false);

        assert_eq!(date_ranges_overlap(r2_before, None, r1_start, Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(r2_between, None, r1_start, Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(r2_after, None, r1_start, Some(r1_end)), false);

        // Test when both ranges have end dates - the non-overlapping cases should be 
        // when either ranges' start date is after the other one's end date
        assert_eq!(date_ranges_overlap(r1_start, Some(r1_end), r2_before, Some(r2_before2)), false);
        assert_eq!(date_ranges_overlap(r1_start, Some(r1_end), r2_before, Some(r2_between)), true);
        assert_eq!(date_ranges_overlap(r1_start, Some(r1_end), r2_between, Some(r2_after)), true);
        assert_eq!(date_ranges_overlap(r1_start, Some(r1_end), r2_after, Some(r2_after2)), false);
        
        assert_eq!(date_ranges_overlap(r2_before, Some(r2_before2), r1_start, Some(r1_end)), false);
        assert_eq!(date_ranges_overlap(r2_before, Some(r2_between), r1_start, Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(r2_between, Some(r2_after), r1_start, Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(r2_after, Some(r2_after2), r1_start, Some(r1_end)), false);

        Ok(())
    }
}