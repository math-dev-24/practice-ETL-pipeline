mod storage;
mod models;
mod utils;

use std::sync::{Arc, Mutex};
use std::time::Instant;
use crate::models::csv_multi_reader::MultiCsvReader;
use crate::models::error::ValidationError;
use crate::storage::sqlite::Database;
use crate::models::user::User;
use crate::models::pipeline::{Pipeline};
use crate::utils::capitalize::capitalize;
use crate::utils::multi_extract::{multi_extract, multi_extract_streaming};
use crate::utils::set_user::generate_user;
// 12-14 sec de traitements

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let paths: Vec<&str> = vec![
        "./src/data/data_1.csv",
        "./src/data/data_2.csv",
        "./src/data/data_3.csv",
        "./src/data/data_4.csv",
        "./src/data/data_5.csv"
    ];

    // Cas sans chunk

    println!("=========== Cas chunk ==========");

    let mut time_start = Instant::now();

    let db_normal = Database::new("./output_normal.db");
    db_normal.init()?;

    let merged_pipeline = multi_extract(&paths)?;

    let pipeline = merged_pipeline
        .transform(generate_user)
        .transform_if(
            |user| user.first_name.chars().next().unwrap_or('A').is_lowercase(),
            |mut user| {
                user.first_name = capitalize(&user.first_name);
                user
            }
        )
        .filter(|user| {
            match user.is_valid() {
                Ok(_) => true,
                Err(_) => {
                    false
                },
            }
        });

    db_normal.insert_user(&pipeline.data)?;

    println!("Elapsed time: {:?}", time_start.elapsed());
    println!("Stat normal : {:?}", pipeline.stats);


    println!("=========== Cas chunk ==========");
    // Cas Chunk
    time_start = Instant::now();

    let db_streaming = Database::new("./output-streaming.db");
    db_streaming.init()?;

    let multi_reader = multi_extract_streaming(&paths, 1000)?;

    let stats = multi_reader
        .transform(generate_user)
        .filter(|user| user.is_valid().is_ok())
        .load(|users| {
            db_streaming.insert_user(users)?;
            Ok(())
        })?;

    println!("Streaming with DB: {:?}", time_start.elapsed());
    println!("Stat Streaming : {:?}", stats);

    Ok(())
}
