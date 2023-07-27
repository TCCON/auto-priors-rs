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


pub struct DateIterator {
    date_ranges: Vec<(NaiveDate, NaiveDate)>,
    curr_date: Option<NaiveDate>,
    range_idx: usize,
    not_before: Option<NaiveDate>,
    not_after: Option<NaiveDate>,
    first: bool,
}

impl DateIterator {
    pub fn new(date_ranges: Vec<(NaiveDate, NaiveDate)>) -> Self {
        Self { date_ranges, curr_date: None, range_idx: 0, not_before: None, not_after: None, first: true }
    }

    pub fn new_with_bounds(date_ranges: Vec<(NaiveDate, NaiveDate)>, not_before: Option<NaiveDate>, not_after: Option<NaiveDate>) -> Self {
        Self { date_ranges, curr_date: None, range_idx: 0, not_before, not_after, first: true }

    }
}

impl Iterator for DateIterator {
    type Item = NaiveDate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.date_ranges.is_empty() {
            return None
        }

        loop {
            if self.first {
                self.curr_date = Some(self.date_ranges[0].0);
                self.first = false;
            } else if let Some(d) = self.curr_date {
                let next_date = d + Duration::days(1);
                let is_range_end = next_date == self.date_ranges[self.range_idx].1;
                if is_range_end && self.range_idx == self.date_ranges.len() - 1 {
                    self.curr_date = None;
                } else if is_range_end {
                    self.range_idx += 1;
                    self.curr_date = Some(self.date_ranges[self.range_idx].0);
                } else {
                    self.curr_date = Some(next_date);
                }
            }

            if let Some(d) = self.curr_date {
                let before_start = self.not_before.map_or(false, |b| d < b);
                let after_end = self.not_after.map_or(false, |a| d >= a);
                if ! before_start && ! after_end {
                    break;
                }
            } else {
                break;
            }
        }

        self.curr_date
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn test_date_range_overlap() -> anyhow::Result<()> {
        let r1_start = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        let r1_end = NaiveDate::from_ymd_opt(2010, 1, 31).unwrap();
        let r2_before = NaiveDate::from_ymd_opt(2009, 12, 1).unwrap();
        let r2_before2 = NaiveDate::from_ymd_opt(2009, 12, 15).unwrap();
        let r2_between = NaiveDate::from_ymd_opt(2010, 1, 15).unwrap();
        let r2_after = NaiveDate::from_ymd_opt(2010, 2, 15).unwrap();
        let r2_after2 = NaiveDate::from_ymd_opt(2010, 3, 1).unwrap();
        
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


    #[test]
    fn test_date_iterator_empty() {
        let mut it = DateIterator::new(vec![]);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_date_iterator_single_range() {
        let it = DateIterator::new(vec![
            (NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 4).unwrap())
        ]);

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()]);
    }

    #[test]
    fn test_date_iterator_single_range_with_before() {
        let it = DateIterator::new_with_bounds(
            vec![(NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 4).unwrap())],
            Some(NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()), None
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()]);
    }

    #[test]
    fn test_date_iterator_single_range_with_after() {
        let it = DateIterator::new_with_bounds(
            vec![(NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 4).unwrap())],
            None, Some(NaiveDate::from_ymd_opt(2018, 1, 3).unwrap())
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()]);
    }

    #[test]
    fn test_date_iterator_single_range_with_before_and_after() {
        let it = DateIterator::new_with_bounds(
            vec![(NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 10).unwrap())],
            Some(NaiveDate::from_ymd_opt(2018, 1, 2).unwrap()), Some(NaiveDate::from_ymd_opt(2018, 1, 4).unwrap())
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()]);
    }

    #[test]
    fn test_date_iterator_multi_range() {
        let it = DateIterator::new(vec![
            (NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 4).unwrap()),
            (NaiveDate::from_ymd_opt(2020, 7, 31).unwrap(), NaiveDate::from_ymd_opt(2020, 8, 2).unwrap()),
            (NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(), NaiveDate::from_ymd_opt(2021, 1, 2).unwrap()),
        ]);

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 3).unwrap(), 
                           NaiveDate::from_ymd_opt(2020, 7, 31).unwrap(), NaiveDate::from_ymd_opt(2020, 8, 1).unwrap(),
                           NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(), NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()]);
    }

    #[test]
    fn test_date_iterator_multi_range_with_bounds() {
        let it = DateIterator::new_with_bounds(vec![
                (NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 4).unwrap()),
                (NaiveDate::from_ymd_opt(2020, 7, 31).unwrap(), NaiveDate::from_ymd_opt(2020, 8, 2).unwrap()),
                (NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(), NaiveDate::from_ymd_opt(2021, 1, 2).unwrap()),
            ],
            Some(NaiveDate::from_ymd_opt(2020, 7, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap())
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2020, 7, 31).unwrap(), NaiveDate::from_ymd_opt(2020, 8, 1).unwrap(), NaiveDate::from_ymd_opt(2020, 12, 31).unwrap()]);
    }

    #[test]
    fn test_date_iterator_unorded_ranges() {
        let it = DateIterator::new(vec![
            (NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(), NaiveDate::from_ymd_opt(2021, 1, 2).unwrap()),
            (NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 4).unwrap()),
            (NaiveDate::from_ymd_opt(2020, 7, 31).unwrap(), NaiveDate::from_ymd_opt(2020, 8, 2).unwrap()),
        ]);

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(), NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
                           NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 3).unwrap(), 
                           NaiveDate::from_ymd_opt(2020, 7, 31).unwrap(), NaiveDate::from_ymd_opt(2020, 8, 1).unwrap()]);
    }
}