use std::{fmt::Display, str::FromStr};

use chrono::{NaiveDate, Duration};
use itertools::Itertools;

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
/// let d1 = NaiveDate::from_ymd_opt(2010, 1, 1);
/// let d2 = NaiveDate::from_ymd_opt(2010, 2, 1);
/// 
/// assert_eq!(date_ranges_overlap(d1, d2, d2, None), false);
/// ```
/// 
/// # Parameters
/// * `r1_start`, `r2_start` - first date in each range.
/// * `r1_end`, `r2_end` - last date (exclusive) in each range. If the range is
///   open-ended, pass `None`.
/// 
/// # Returns
/// `true` if the ranges overlap by at least 1 day, `false` otherwise.
pub fn date_ranges_overlap(r1_start: Option<NaiveDate>, r1_end: Option<NaiveDate>, r2_start: Option<NaiveDate>, r2_end: Option<NaiveDate>) -> bool {
    DateRangeOverlap::classify(r1_start, r1_end, r2_start, r2_end).has_overlap()
}


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DateRangeOverlap {
    /// The second date range is fully within the first, meaning that all dates from the second are also in the first
    AContainsB,

    /// The first date range is fully within the second, meaning that all dates from the first are also in the second
    AInsideB,

    /// The end of the first date range overlaps with the start of the second. This does *not* includes the case where
    /// the start dates are the same in both ranges and the first range ends before the second (that is considered
    /// `AContainsB`). Note that the end date of the first must be greater than (not equal to) the start date of the 
    /// second, as date range end dates are assumed to be exclusive.
    AEndsInB,

    /// The start of the first date range overlaps the end of the second. This does *not* include the case where the
    /// end dates are the same in both ranges and the first starts after the second (that is considered `AContainsB`). 
    /// Note that the end date of the second must be greater than (not equal to) the start date of the first, as end 
    /// dates are assumed to be exclusive.
    AStartsInB,

    /// The bounds of both date ranges are exactly the same.
    AEqualsB,

    /// There is no overlap between the two date ranges; if the end date of one range equals the start date
    /// of another, that is no overlap because date ranges are assumed to be exclusive.
    None
}

impl DateRangeOverlap {
    pub fn has_overlap(&self) -> bool {
        self != &Self::None
    }

    pub fn classify(r1_start: Option<NaiveDate>, r1_end: Option<NaiveDate>, r2_start: Option<NaiveDate>, r2_end: Option<NaiveDate>) -> Self {
        match (r1_start, r1_end, r2_start, r2_end) {
            (None, None, None, None) => DateRangeOverlap::AEqualsB,
            (None, None, None, Some(_)) => DateRangeOverlap::AContainsB,
            (None, None, Some(_), None) => DateRangeOverlap::AContainsB,
            (None, None, Some(_), Some(_)) => DateRangeOverlap::AContainsB,
            (None, Some(_), None, None) => DateRangeOverlap::AEndsInB,
            (None, Some(e1), None, Some(e2)) => {
                if e1 == e2 {
                    DateRangeOverlap::AEqualsB
                } else if e1 < e2 {
                    DateRangeOverlap::AInsideB
                } else {
                    DateRangeOverlap::AContainsB
                }
            },
            (None, Some(e1), Some(s2), None) => {
                if e1 <= s2 {
                    DateRangeOverlap::None
                } else {
                    DateRangeOverlap::AEndsInB
                }
            },
            (None, Some(e1), Some(s2), Some(e2)) => {
                if e1 <= s2 {
                    DateRangeOverlap::None
                } else if e1 > s2 && e1 < e2 {
                    DateRangeOverlap::AEndsInB
                } else {
                    DateRangeOverlap::AContainsB
                }
            },
            (Some(_), None, None, None) => DateRangeOverlap::AStartsInB,
            (Some(s1), None, None, Some(e2)) => {
                if s1 >= e2 {
                    DateRangeOverlap::None
                } else {
                    DateRangeOverlap::AStartsInB
                }
            },
            (Some(s1), None, Some(s2), None) => {
                if s1 == s2 {
                    DateRangeOverlap::AEqualsB
                } else if s1 < s2 {
                    DateRangeOverlap::AContainsB
                } else {
                    DateRangeOverlap::AInsideB
                }
            },
            (Some(s1), None, Some(s2), Some(e2)) => {
                if s1 <= s2 {
                    DateRangeOverlap::AContainsB
                } else if s1 < e2 {
                    DateRangeOverlap::AStartsInB
                } else {
                    DateRangeOverlap::None
                }
            },
            (Some(_), Some(_), None, None) => DateRangeOverlap::AInsideB,
            (Some(s1), Some(e1), None, Some(e2)) => {
                if e2 <= s1 {
                    DateRangeOverlap::None
                } else if e1 <= e2 {
                    DateRangeOverlap::AInsideB
                } else {
                    DateRangeOverlap::AStartsInB
                }
            },
            (Some(s1), Some(e1), Some(s2), None) => {
                if s2 >= e1 {
                    DateRangeOverlap::None
                } else if s1 >= s2 {
                    DateRangeOverlap::AInsideB
                } else {
                    DateRangeOverlap::AEndsInB
                }
            },
            (Some(s1), Some(e1), Some(s2), Some(e2)) => {
                if s1 == s2 && e1 == e2 {
                    DateRangeOverlap::AEqualsB
                } else if s1 <= s2 && e1 >= e2 {
                    DateRangeOverlap::AContainsB
                } else if s1 < s2 && e1 > s2 && e1 <= e2 {
                    DateRangeOverlap::AEndsInB
                } else if s1 > s2 && s1 < e2 && e1 > e2 {
                    DateRangeOverlap::AStartsInB
                } else if s1 >= e2 || s2 >= e1 {
                    DateRangeOverlap::None
                } else {
                    DateRangeOverlap::AInsideB
                }
            },
        }
    }
}



/// An iterator over one or more date ranges.
/// 
/// Note that this should always be constructed through one of the `new*`
/// methods, rather than directly. Doing so ensures that invalid ranges are 
/// always filtered out.
pub struct DateIterator {
    date_ranges: Vec<(NaiveDate, NaiveDate)>,
    curr_date: Option<NaiveDate>,
    range_idx: usize,
    not_before: Option<NaiveDate>,
    not_after: Option<NaiveDate>,
    first: bool,
}

impl DateIterator {
    /// Create a new date iterator over one or more date ranges
    /// 
    /// The input `date_ranges` is a vector of tuples each containing two [`NaiveDate`]s. Each tuple defines
    /// the start and end date of a date range (end date is exclusive).
    /// 
    /// # Example
    /// ```
    /// use chrono::NaiveDate;
    /// 
    /// let ranges = vec![
    ///     (NaiveDate::from_ymd_opt(2010,1,1).unwrap(), NaiveDate::from_ymd_opt(2010,1,3)),
    ///     (NaiveDate::from_ymd_opt(2010,1,30).unwrap(), NaiveDate::from_ymd_opt(2010,2,2)),
    /// ]
    /// 
    /// let iter_dates: Vec<_> = DateIterator::new(ranges).collect();
    /// let expected_dates = vec![
    ///     NaiveDate::from_ymd_opt(2010,1,1).unwrap(),
    ///     NaiveDate::from_ymd_opt(2010,1,2).unwrap(),
    ///     NaiveDate::from_ymd_opt(2010,1,30).unwrap(),
    ///     NaiveDate::from_ymd_opt(2010,1,31).unwrap(),
    ///     NaiveDate::from_ymd_opt(2010,2,1).unwrap(),
    /// ];
    /// 
    /// assert_eq!(iter_dates, expected_dates);
    /// ```
    pub fn new(date_ranges: Vec<(NaiveDate, NaiveDate)>) -> Self {
        let date_ranges = Self::filter_empty_ranges(date_ranges);
        Self { date_ranges, curr_date: None, range_idx: 0, not_before: None, not_after: None, first: true }
    }

    pub fn new_one_range(start_date: NaiveDate, end_date: NaiveDate) -> Self {
        Self::new(vec![(start_date, end_date)])
    }

    /// Create a new date iterator that will skip dates before and/or after given dates
    /// 
    /// # Inputs
    /// - `date_ranges`: same as for [`new`]
    /// - `not_before`: if this is `Some(date)` then the iterator will start on `date`
    /// - `not_after`: if this is `Some(date)` then the iterator will stop on the *day before* `date`
    pub fn new_with_bounds(date_ranges: Vec<(NaiveDate, NaiveDate)>, not_before: Option<NaiveDate>, not_after: Option<NaiveDate>) -> Self {
        let date_ranges = Self::filter_empty_ranges(date_ranges);
        Self { date_ranges, curr_date: None, range_idx: 0, not_before, not_after, first: true }

    }

    fn filter_empty_ranges(date_ranges: Vec<(NaiveDate, NaiveDate)>) -> Vec<(NaiveDate, NaiveDate)> {
        // This is necessary because having a range where the end date is <= the start date incorrectly
        // causes the main loop to iterator the first date, regardless. The logic to advance dates is
        // complex enough it's easier just to filter out invalid ranges.
        date_ranges.into_iter()
            .filter(|(a,b)| b > a)
            .collect_vec()
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

pub fn format_lat_str(lat: f32, prec: u8) -> String {
    let ns = if lat >= 0.0 { "N" } else { "S" };
    let lat = lat.abs();
    match prec {
        0 => format!("{lat:.0}{ns}"),
        1 => format!("{lat:.1}{ns}"),
        2 => format!("{lat:.2}{ns}"),
        3 => format!("{lat:.3}{ns}"),
        4 => format!("{lat:.4}{ns}"),
        5 => format!("{lat:.5}{ns}"),
        6 => format!("{lat:.6}{ns}"),
        7 => format!("{lat:.7}{ns}"),
        _ => unimplemented!("precision > 7 not implemented")
    }
}

pub fn format_lon_str(lon: f32, prec: u8) -> String {
    let lon = if lon > 180.0 { lon - 360.0 } else { lon };
    let ew = if lon >= 0.0 { "E" } else { "W" };
    let lon = lon.abs();
    match prec {
        0 => format!("{lon:.0}{ew}"),
        1 => format!("{lon:.1}{ew}"),
        2 => format!("{lon:.2}{ew}"),
        3 => format!("{lon:.3}{ew}"),
        4 => format!("{lon:.4}{ew}"),
        5 => format!("{lon:.5}{ew}"),
        6 => format!("{lon:.6}{ew}"),
        7 => format!("{lon:.7}{ew}"),
        _ => unimplemented!("precision > 7 not implemented")
    }
}

#[derive(Debug)]
pub struct ParseInputBoolError(String);

impl Display for ParseInputBoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Given string ('{}') was not any of 'true', 'yes', 'y', 'false', 'no', 'n' (ignoring case)", self.0)
    }
}

pub fn parse_bool_str(s: &str) -> Result<bool, ParseInputBoolError> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "yes" | "y" => Ok(true),
        "false" | "no" | "n" => Ok(false),
        _ => Err(ParseInputBoolError(s.to_string()))
    }
}

pub fn table_to_std_string(mut tab: tabled::Table) -> String {
    let table_config = tabled::settings::Settings::default()
        .with(tabled::settings::Style::markdown());
    tab.with(table_config);
    tab.to_string()
}

pub fn to_std_table<I, T>(iter: I) -> String 
where
    I: IntoIterator<Item = T>,
    T: tabled::Tabled
{
    let table_config = tabled::settings::Settings::default()
        .with(tabled::settings::Style::markdown());
    tabled::Table::new(iter)
        .with(table_config)
        .to_string()
}

pub fn is_valid_email(email: &str) -> bool {
    lettre::Address::from_str(email).is_ok()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn date(y: i32, m: u32, d: u32) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(y, m, d)
    }

    #[test]
    fn test_date_range_overlap_bool() -> anyhow::Result<()> {
        let r1_start = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        let r1_end = NaiveDate::from_ymd_opt(2010, 1, 31).unwrap();
        let r2_before = NaiveDate::from_ymd_opt(2009, 12, 1).unwrap();
        let r2_before2 = NaiveDate::from_ymd_opt(2009, 12, 15).unwrap();
        let r2_between = NaiveDate::from_ymd_opt(2010, 1, 15).unwrap();
        let r2_after = NaiveDate::from_ymd_opt(2010, 2, 15).unwrap();
        let r2_after2 = NaiveDate::from_ymd_opt(2010, 3, 1).unwrap();
        
        // Test when both ranges are open ended, making sure that the result is symmetrical
        assert_eq!(date_ranges_overlap(Some(r1_start), None, Some(r2_before), None), true);
        assert_eq!(date_ranges_overlap(Some(r1_start), None, Some(r2_between), None), true);
        assert_eq!(date_ranges_overlap(Some(r1_start), None, Some(r2_after), None), true);

        assert_eq!(date_ranges_overlap(Some(r2_before), None, Some(r1_start), None), true);
        assert_eq!(date_ranges_overlap(Some(r2_between), None, Some(r1_start), None), true);
        assert_eq!(date_ranges_overlap(Some(r2_after), None, Some(r1_start), None), true);
        
        // Test when one range has an end date - the only non-overlapping cases should be
        // when the start date of the open ended range is after the end date of the closed
        // range.
        assert_eq!(date_ranges_overlap(Some(r1_start), Some(r1_end), Some(r2_before), None), true);
        assert_eq!(date_ranges_overlap(Some(r1_start), Some(r1_end), Some(r2_between), None), true);
        assert_eq!(date_ranges_overlap(Some(r1_start), Some(r1_end), Some(r2_after), None), false);

        assert_eq!(date_ranges_overlap(Some(r2_before), None, Some(r1_start), Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(Some(r2_between), None, Some(r1_start), Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(Some(r2_after), None, Some(r1_start), Some(r1_end)), false);

        // Test when both ranges have end dates - the non-overlapping cases should be 
        // when either ranges' start date is after the other one's end date
        assert_eq!(date_ranges_overlap(Some(r1_start), Some(r1_end), Some(r2_before), Some(r2_before2)), false);
        assert_eq!(date_ranges_overlap(Some(r1_start), Some(r1_end), Some(r2_before), Some(r2_between)), true);
        assert_eq!(date_ranges_overlap(Some(r1_start), Some(r1_end), Some(r2_between), Some(r2_after)), true);
        assert_eq!(date_ranges_overlap(Some(r1_start), Some(r1_end), Some(r2_after), Some(r2_after2)), false);
        
        assert_eq!(date_ranges_overlap(Some(r2_before), Some(r2_before2), Some(r1_start), Some(r1_end)), false);
        assert_eq!(date_ranges_overlap(Some(r2_before), Some(r2_between), Some(r1_start), Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(Some(r2_between), Some(r2_after), Some(r1_start), Some(r1_end)), true);
        assert_eq!(date_ranges_overlap(Some(r2_after), Some(r2_after2), Some(r1_start), Some(r1_end)), false);

        Ok(())
    }

    #[test]
    fn test_date_range_overlap_classification() {
        // A == B
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), date(2010, 1, 31), date(2010, 1, 1), date(2010, 1, 31)), DateRangeOverlap::AEqualsB);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 1, 31), None, date(2010, 1, 31)), DateRangeOverlap::AEqualsB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), None, date(2010, 1, 1), None), DateRangeOverlap::AEqualsB);
        assert_eq!(DateRangeOverlap::classify(None, None, None, None), DateRangeOverlap::AEqualsB);

        // A contains B
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), date(2010, 12, 31), date(2010, 6, 1), date(2010, 6, 30)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 12, 31), date(2010, 6, 1), date(2010, 6, 30)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), None, date(2010, 6, 1), date(2010, 6, 30)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(None, None, date(2010, 6, 1), date(2010, 6, 30)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 12, 31), None, date(2010, 6, 30)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), None, date(2010, 6, 1), None), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(None, None, None, date(2010, 6, 30)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(None, None, date(2010, 6, 1), None), DateRangeOverlap::AContainsB);

        // (edge cases with equal start or end dates)
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 6, 1), date(2010, 1, 1), date(2010, 6, 1)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), None, date(2010, 1, 1), date(2010, 6, 1)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(date(2004, 7, 1), date(2005, 1, 1), date(2004, 7, 1), date(2004, 8, 1)), DateRangeOverlap::AContainsB);
        assert_eq!(DateRangeOverlap::classify(date(2004, 7, 1), date(2005, 1, 1), date(2004, 12, 1), date(2005, 1, 1)), DateRangeOverlap::AContainsB);

        // A inside B
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), date(2010, 6, 30), date(2010, 1, 1), date(2010, 12, 31)), DateRangeOverlap::AInsideB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), date(2010, 6, 30), None, date(2010, 12, 31)), DateRangeOverlap::AInsideB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), date(2010, 6, 30), date(2010, 1, 1), None), DateRangeOverlap::AInsideB);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 6, 30), None, date(2010, 12, 31)), DateRangeOverlap::AInsideB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), None, date(2010, 1, 1), None), DateRangeOverlap::AInsideB);
        
        // (edge cases with equal start or end dates)
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), date(2010, 6, 1), date(2010, 1, 1), None), DateRangeOverlap::AInsideB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), date(2010, 6, 1), None, date(2010, 6, 1)), DateRangeOverlap::AInsideB);

        // These are cases that showed up when clearing site info, that I just want to check
        assert_eq!(DateRangeOverlap::classify(date(2017, 1, 1), date(2017, 12, 1), date(2017, 1, 1), None), DateRangeOverlap::AInsideB);
        assert_eq!(DateRangeOverlap::classify(date(2004, 12, 1), date(2005, 1, 1), date(2004, 7, 1), date(2005, 1, 1)), DateRangeOverlap::AInsideB);

        // A ends in B
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), date(2010, 9, 1), date(2010, 3, 1), date(2010, 12, 31)), DateRangeOverlap::AEndsInB);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 9, 1), date(2010, 3, 1), date(2010, 12, 31)), DateRangeOverlap::AEndsInB);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 9, 1), date(2010, 3, 1), None), DateRangeOverlap::AEndsInB);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 9, 1), None, None), DateRangeOverlap::AEndsInB);

        // A starts in B
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), date(2010, 12, 31), date(2010, 1, 1), date(2010, 9, 1)), DateRangeOverlap::AStartsInB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), None, date(2010, 1, 1), date(2010, 9, 1)), DateRangeOverlap::AStartsInB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), None, None, date(2010, 9, 1)), DateRangeOverlap::AStartsInB);
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), None, None, None), DateRangeOverlap::AStartsInB);

        // No overlap
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), date(2010, 3, 1), date(2010, 6, 1), date(2010, 9, 1)), DateRangeOverlap::None);
        assert_eq!(DateRangeOverlap::classify(date(2010, 6, 1), date(2010, 9, 1), date(2010, 1, 1), date(2010, 3, 1)), DateRangeOverlap::None);
        assert_eq!(DateRangeOverlap::classify(None, date(2010, 3, 1), date(2010, 6, 1), date(2010, 12, 1)), DateRangeOverlap::None);
        assert_eq!(DateRangeOverlap::classify(date(2010, 1, 1), date(2010, 3, 1), date(2010, 6, 1), None), DateRangeOverlap::None);

    }


    #[test]
    fn test_date_iterator_empty() {
        let mut it = DateIterator::new(vec![]);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_date_iterator_start_equal_end() {
        let mut it = DateIterator::new(vec![(NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 1).unwrap())]);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_date_iterator_one_day() {
        let it = DateIterator::new_one_range(NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 2).unwrap());
        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()]);
    }

    #[test]
    fn test_date_iterate_second_range_start_equal_end() {
        let it = DateIterator::new(vec![
            (NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 3).unwrap()),
            (NaiveDate::from_ymd_opt(2018, 6, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 6, 1).unwrap()),
            (NaiveDate::from_ymd_opt(2018, 12, 31).unwrap(), NaiveDate::from_ymd_opt(2019, 1, 1).unwrap())
        ]);
        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(), NaiveDate::from_ymd_opt(2018, 12, 31).unwrap()]);
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