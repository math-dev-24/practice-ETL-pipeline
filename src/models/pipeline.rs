use std::collections::HashMap;
use rayon::prelude::*;

#[derive(Debug, Default)]
pub struct PipelineStats {
    pub total_extracted: usize,
    pub total_transformed: usize,
    pub total_filtered: usize,
    errors: Vec<String>
}

pub struct Pipeline<T> {
    pub data: Vec<T>,
    pub stats: PipelineStats
}

impl Pipeline<csv::StringRecord> {
    pub fn extract(source: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b';')
            .from_path(source)?;

        let mut data = Vec::new();
        let mut errors = Vec::new();

        for (idx, result) in reader.records().enumerate() {
            match result {
                Ok(record) => {
                    data.push(record)
                }
                Err(err) => {
                    errors.push(format!("{} record parse error: {}", idx, err));
                }
            }
        }

        let default_stats = PipelineStats::default();
        let count = data.len();

        Ok(Pipeline {
            data,
            stats: PipelineStats{
                errors,
                total_extracted: count,
                ..default_stats
            }
        })
    }
}


impl<T: Send + Sync> Pipeline<T> {
    pub fn transform<F, U>(self, f: F) -> Pipeline<U>
    where
        F: Fn(T) -> U + Sync + Send,
        T: Send,
        U: Send
    {
        let transformed: Vec<U> = self.data
            .into_par_iter()
            .map(f)
            .collect();

        let count = transformed.len();

        Pipeline {
            data: transformed,
            stats: PipelineStats {
                total_transformed: count,
                ..self.stats
            }
        }
    }

    pub fn filter<F>(self, predicate: F) -> Pipeline<T>
    where F: Fn(&T) -> bool + Sync + Send
    {
        let filtered: Vec<T> = self.data
            .into_par_iter()
            .filter(predicate).collect();

        let count = filtered.len();

        Pipeline {
            data: filtered,
            stats: PipelineStats {
                total_filtered: count,
                ..self.stats
            }
        }
    }

    pub fn aggregate<K>(self, key_fn: impl Fn(&T) -> K + Sync + Send) -> HashMap<K, usize>
    where
        K: Eq + std::hash::Hash + Send + Clone
    {
        self.data.par_iter()
            .fold(
                || HashMap::new(),  // Chaque thread crée son HashMap
                |mut map, item| {   // Chaque thread accumule dans SON map
                    let key = key_fn(item);
                    *map.entry(key).or_insert(0) += 1;
                    map  // Retourne le map modifié
                }
            )
            // À ce stade : plusieurs HashMaps partiels
            .reduce(
                || HashMap::new(),
                |mut map1, map2| {  // Fusionne deux HashMaps
                    for (k, v) in map2 {
                        *map1.entry(k).or_insert(0) += v;
                    }
                    map1
                }
            )
    }

    pub fn merge(mut self, other: Pipeline<T>) -> Pipeline<T> {
        self.data.extend(other.data);

        self.stats.total_extracted += other.stats.total_extracted;
        self.stats.total_transformed += other.stats.total_transformed;
        self.stats.total_filtered += other.stats.total_filtered;
        self.stats.errors.extend(other.stats.errors);

        self
    }

    pub fn report(&self) {
        println!("=== Pipeline Statistics ===");
        println!("📥 Extracted: {}", self.stats.total_extracted);
        println!("🔄 Transformed: {}", self.stats.total_transformed);
        println!("✅ Filtered (kept): {}", self.stats.total_filtered);
        println!("❌ Rejected: {}", self.stats.total_transformed - self.stats.total_filtered);
        if !self.stats.errors.is_empty() {
            println!("⚠️  Errors: {}", self.stats.errors.len());
            for err in &self.stats.errors {
                println!("   - {}", err);
            }
        }
    }

}