use std::sync::{Arc, Mutex};
use rayon::prelude::*;

use crate::models::csv_reader::CsvReader;
use crate::models::pipeline::PipelineStats;

pub struct StreamingPipeline<I, T>
where
    I: Iterator<Item = Vec<T>> + Send,
    T: Send + Sync
{
    pub chunks: I,
    pub stats: PipelineStats
}


impl StreamingPipeline<CsvReader, csv::StringRecord> {
    pub fn extract_streaming(path: &str, chunk_size: usize) -> Result<Self, Box<dyn std::error::Error>>
    {
        let reader = CsvReader::new(path, chunk_size)?;
        Ok(
            StreamingPipeline {
                chunks: reader,
                stats: PipelineStats::default(),
            }
        )
    }
}

impl <I, T> StreamingPipeline<I, T>
where
    I: Iterator<Item = Vec<T>> + Send,
    T: Send + Sync
{
    pub fn transform<F, U>(self, f: F) -> StreamingPipeline<impl Iterator<Item = Vec<U>>, U>
    where
        F: Fn(T) -> U + Send + Sync,
        U: Send + Sync
    {
        let transformed_chunks = self.chunks
            .map(move |chunk| {
                chunk.into_iter()
                    .map(|item| f(item))
                    .collect::<Vec<U>>()
            });

        StreamingPipeline {
            chunks: transformed_chunks,
            stats: self.stats
        }
    }

    pub fn filter<P>(self, predicate: P) -> StreamingPipeline<impl Iterator<Item = Vec<T>>, T>
    where
        P: Fn(&T) -> bool + Send + Sync + 'static
    {
        let pred = Arc::new(predicate);

        let filtered_chunk = self.chunks.map(move |chunk| {
            let pred = pred.clone(); // Clone l'Arc (pas la closure)
            chunk.into_iter().filter(move |item| pred(item)).collect()
        });

        StreamingPipeline {
            chunks: filtered_chunk,
            stats: self.stats
        }
    }

    pub fn load<F>(mut self, mut loader: F) -> Result<PipelineStats, Box<dyn std::error::Error>>
    where
        F: FnMut(&[T]) -> Result<(), Box<dyn std::error::Error>>
    {
        for chunk in self.chunks {
            loader(&chunk)?;
            self.stats.total_filtered += chunk.len();
        }

        Ok(self.stats)
    }
}

#[cfg(test)]
mod test {
    use std::time::Instant;
    use crate::storage::sqlite::Database;
    use crate::utils::set_user::generate_user;
    use super::*;

    #[test]
    fn test_streaming_pipeline_full() -> Result<(), Box<dyn std::error::Error>> {

        let start = Instant::now();

        let total_user: usize = 43000;

        let db = Database::new("./output.db");
        db.init()?;

        let stats = StreamingPipeline::extract_streaming("./src/data/data_1.csv", 1000)?
        .transform(generate_user)
        .filter(|user| user.is_valid().is_ok())
            .load(|users| {
                println!("Inserting batch of {} users", users.len());
                db.insert_user(users)?;
                Ok(())
            })?;

        assert_eq!(stats.total_filtered, total_user);

        let total_user = db.get_all_users()?.len();

        assert_eq!(total_user, total_user);

        println!("Finished in {}ms", start.elapsed().as_millis());

        Ok(())
    }
}
