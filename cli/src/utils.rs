use std::{io::Write, process::Command};
use std::path::Path;

use anyhow::{Result, Context};
use chrono::{NaiveDate, Duration};

pub trait Downloader {
    fn add_file_to_download(&mut self, url: String) -> Result<()>;
    fn download_files(&mut self, save_dir: &Path) -> Result<()>;
    fn iter_files(&self) -> std::slice::Iter<'_, String>;
}

#[derive(Debug, Clone)]
pub struct WgetDownloader {
    urls: Vec<String>
}

impl WgetDownloader {
    pub fn new() -> Self {
        Self { urls: Vec::new() }
    }
}

impl Downloader for WgetDownloader {
    fn add_file_to_download(&mut self, url: String) -> Result<()> {
        self.urls.push(url);
        Ok(())
    }

    fn download_files(&mut self, save_dir: &Path) -> Result<()> {
        let wget_list = save_dir.join("wget_list.txt");
        let mut f = std::fs::File::create(&wget_list)
            .with_context(|| format!("Unable to create file for list of URLs for wget to {}", wget_list.display()))?;
        for url in self.urls.iter() {
            writeln!(f, "{}", url).with_context(|| format!("Unable to write URL to wget list {}", wget_list.display()))?;
        }

        Command::new("wget")
            .args(["-i", "wget_list.txt"])
            .current_dir(&save_dir)
            .spawn()
            .with_context(|| format!("wget command to download in {} failed", save_dir.display()))?;
        
            Ok(())
    }

    fn iter_files(&self) -> std::slice::Iter<'_, String>{
        self.urls.iter()
    }
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
    use super::*;

    #[test]
    fn test_date_iterator_empty() {
        let mut it = DateIterator::new(vec![]);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_date_iterator_single_range() {
        let it = DateIterator::new(vec![
            (NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 4))
        ]);

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 2), NaiveDate::from_ymd(2018, 1, 3)]);
    }

    #[test]
    fn test_date_iterator_single_range_with_before() {
        let it = DateIterator::new_with_bounds(
            vec![(NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 4))],
            Some(NaiveDate::from_ymd(2018, 1, 2)), None
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd(2018, 1, 2), NaiveDate::from_ymd(2018, 1, 3)]);
    }

    #[test]
    fn test_date_iterator_single_range_with_after() {
        let it = DateIterator::new_with_bounds(
            vec![(NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 4))],
            None, Some(NaiveDate::from_ymd(2018, 1, 3))
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 2)]);
    }

    #[test]
    fn test_date_iterator_single_range_with_before_and_after() {
        let it = DateIterator::new_with_bounds(
            vec![(NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 10))],
            Some(NaiveDate::from_ymd(2018, 1, 2)), Some(NaiveDate::from_ymd(2018, 1, 4))
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd(2018, 1, 2), NaiveDate::from_ymd(2018, 1, 3)]);
    }

    #[test]
    fn test_date_iterator_multi_range() {
        let it = DateIterator::new(vec![
            (NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 4)),
            (NaiveDate::from_ymd(2020, 7, 31), NaiveDate::from_ymd(2020, 8, 2)),
            (NaiveDate::from_ymd(2020, 12, 31), NaiveDate::from_ymd(2021, 1, 2)),
        ]);

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 2), NaiveDate::from_ymd(2018, 1, 3), 
                           NaiveDate::from_ymd(2020, 7, 31), NaiveDate::from_ymd(2020, 8, 1),
                           NaiveDate::from_ymd(2020, 12, 31), NaiveDate::from_ymd(2021, 1, 1)]);
    }

    #[test]
    fn test_date_iterator_multi_range_with_bounds() {
        let it = DateIterator::new_with_bounds(vec![
                (NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 4)),
                (NaiveDate::from_ymd(2020, 7, 31), NaiveDate::from_ymd(2020, 8, 2)),
                (NaiveDate::from_ymd(2020, 12, 31), NaiveDate::from_ymd(2021, 1, 2)),
            ],
            Some(NaiveDate::from_ymd(2020, 7, 1)),
            Some(NaiveDate::from_ymd(2021, 1, 1))
        );

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd(2020, 7, 31), NaiveDate::from_ymd(2020, 8, 1), NaiveDate::from_ymd(2020, 12, 31)]);
    }

    #[test]
    fn test_date_iterator_unorded_ranges() {
        let it = DateIterator::new(vec![
            (NaiveDate::from_ymd(2020, 12, 31), NaiveDate::from_ymd(2021, 1, 2)),
            (NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 4)),
            (NaiveDate::from_ymd(2020, 7, 31), NaiveDate::from_ymd(2020, 8, 2)),
        ]);

        let dates: Vec<NaiveDate> = it.collect();
        assert_eq!(dates, [NaiveDate::from_ymd(2020, 12, 31), NaiveDate::from_ymd(2021, 1, 1),
                           NaiveDate::from_ymd(2018, 1, 1), NaiveDate::from_ymd(2018, 1, 2), NaiveDate::from_ymd(2018, 1, 3), 
                           NaiveDate::from_ymd(2020, 7, 31), NaiveDate::from_ymd(2020, 8, 1)]);
    }
}