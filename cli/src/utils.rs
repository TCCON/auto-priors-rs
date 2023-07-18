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
    urls: Vec<String>,
    verbosity: u8
}

impl WgetDownloader {
    pub fn new() -> Self {
        Self { urls: Vec::new(), verbosity: 1 }
    }

    pub fn new_with_verbosity(verbosity: u8) -> Self {
        Self { urls: Vec::new(), verbosity }
    }

    fn verb_argument(&self) -> &'static str {
        match self.verbosity {
            0 => "--quiet",
            1 => "--no-verbose",
            2 => "",
            _ => "--verbose"
        }
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

        let output = Command::new("wget")
            .args([self.verb_argument(), "-i", "wget_list.txt"])
            .current_dir(&save_dir)
            .output()
            .with_context(|| format!("wget command to download in {} failed to execute", save_dir.display()))?;

        // If these fail, it's not worth propagating that error
        // TODO: maybe pipe to info! or debug!
        let _ = std::io::stdout().write_all(&output.stdout);
        let _ = std::io::stderr().write_all(&output.stderr);
        
        if output.status.success() {
            Ok(())
        } else {
            Err(anyhow::Error::msg(format!("wget call to download files failed with status {}", output.status)))
        }
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