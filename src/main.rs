mod storage;

use std::collections::HashMap;
use std::sync::Mutex;
use std::fs::File;
use rayon::prelude::*;

use crate::storage::init::Database;

#[derive(Debug)]
struct User {
    username: String,
    identifier: String,
    first_name: String,
    last_name: String,
}

impl User {
    fn is_valid(&self) -> bool {
        (!self.first_name.is_empty() || !self.last_name.is_empty() || !self.identifier.is_empty()) && self.username != "username"
    }
}

struct Pipeline<T> {
    data: Vec<T>
}

impl Pipeline<csv::StringRecord> {
    fn extract(source: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b';')
            .from_path(source)?;

        let data = reader.records().collect::<Result<Vec<_>, _>>()?;
        Ok(Pipeline { data })
    }
}


impl<T: Send + Sync> Pipeline<T> {
    fn transform<F, U>(self, f: F) -> Pipeline<U>
    where
        F: Fn(T) -> U + Sync + Send,
        T: Send,
        U: Send
    {
        let transformed = self.data
            .into_par_iter()
            .map(f)
            .collect();

        Pipeline { data: transformed }
    }

    fn load<F>(self, f: F)
    where F: Fn(&T) + Sync + Send
    {
        self.data.par_iter().for_each(f);
    }

    fn filter<F>(self, predicate: F) -> Pipeline<T>
    where F: Fn(&T) -> bool + Sync + Send
    {
        let filtered = self.data
            .into_par_iter()
            .filter(predicate).collect();

        Pipeline { data: filtered }
    }

    fn aggregate<K>(self, key_fn: impl Fn(&T) -> K + Sync + Send) -> HashMap<K, usize>
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

}


fn main() -> Result<(), Box<dyn std::error::Error>> {

    let pipeline = Pipeline::extract("./src/data/username.csv")?
        .transform(|record| {
            User {
                username: record.get(0).unwrap_or("").to_string(),
                identifier: record.get(1).unwrap_or("").to_string(),
                first_name: record.get(2).unwrap_or("").to_string(),
                last_name: record.get(3).unwrap_or("").to_string(),
            }
        })
        .filter(|user| user.is_valid());

    let stat = pipeline.aggregate(|user| {
        user.username.chars().next().unwrap_or('n')
    });

    println!("{:?}", stat);


    // let output = Mutex::new(File::create("output.csv")?);

    // let db = Database::new("./output.db");
    // db.init()?;
    // db.insert_user(&pipeline.data)?;
    // let users_db = db.get_all_users()?;

    Ok(())
}
