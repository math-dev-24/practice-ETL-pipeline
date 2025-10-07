use std::fs::File;


pub struct CsvReader {
    reader: csv::Reader<File>,
    chunk_size: usize,
    current_record: csv::StringRecord,
}

impl CsvReader {
    pub fn new(path: &str, chunk_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let reader = csv::Reader::from_path(path)?;
        Ok(CsvReader {
            reader,
            chunk_size,
            current_record: csv::StringRecord::new()
        })
    }
}

impl Iterator for CsvReader {
    type Item = Vec<csv::StringRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::new();

        for _ in 0..self.chunk_size {
            // Lire dans le buffer current_record
            match self.reader.read_record(&mut self.current_record) {
                Ok(true) => {
                    // Record lu avec succès, le cloner dans le chunk
                    chunk.push(self.current_record.clone());
                }
                Ok(false) => {
                    // Fin du fichier
                    break;
                }
                Err(_) => {
                    // Erreur de lecture, on skip
                    break;
                }
            }
        }

        if chunk.is_empty() {
            None  // Fin de l'iterator
        } else {
            Some(chunk)
        }

    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_extract_chunk() {
        let reader = CsvReader::new("./src/data/data_1.csv", 500).unwrap();

        for (i, chunk) in reader.enumerate() {
            println!("Chunk {}: {} records", i, chunk.len());
        }
    }
}