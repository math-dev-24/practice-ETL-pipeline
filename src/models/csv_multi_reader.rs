use std::error::Error;
use crate::models::csv_reader::CsvReader;

pub struct MultiCsvReader {
    readers: Vec<CsvReader>,
    current_index: usize,
}

impl MultiCsvReader {
    pub fn new(paths: &[&str], chunk_size: usize) -> Result<Self, Box<dyn Error>> {
        let readers: Result<Vec<_>, _> = paths.iter()
            .map(|p| CsvReader::new(p, chunk_size))
            .collect();

        Ok(MultiCsvReader {
            readers: readers?,
            current_index: 0
        })

    }
}

impl Iterator for MultiCsvReader {
    type Item = Vec<csv::StringRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_index >= self.readers.len() {
                return None;
            }

            let current_reader = &mut self.readers[self.current_index];

            match current_reader.next() {
                Some(record) => {
                    return Some(record)
                }
                None => {
                    self.current_index += 1;
                }
            }
        }
    }
}