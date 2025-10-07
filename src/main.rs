mod adapter;
mod models;
mod utils;

use std::time::Instant;
use indicatif::{ProgressBar, ProgressStyle};
use crate::adapter::storage_output::sqlite::SqliteAdapter;
use crate::models::output::OutputPort;
use crate::models::user::User;
use crate::utils::capitalize::capitalize;
use crate::utils::multi_extract::{multi_extract, multi_extract_streaming};
use crate::utils::set_user::generate_user;

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

    let mut db_normal = SqliteAdapter::new("./output_normal.db")?;

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

    let chunk_size_bar = 1000;
    let pb = ProgressBar::new(pipeline.data.len() as u64);

    pb.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
        .unwrap()
        .progress_chars("█▓▒░ ")
    );

    for chunk in pipeline.data.chunks(chunk_size_bar) {
        db_normal.write(chunk)?;
        pb.inc(chunk.len() as u64);
        pb.set_message(format!("Inserting {} chunks", chunk_size_bar));
    }

    pb.finish_with_message("✓ Done!");

    println!("Elapsed time: {:?}", time_start.elapsed());
    println!("Stat normal : {:?}", pipeline.stats);


    println!("=========== Cas chunk ==========");
    // Cas Chunk
    time_start = Instant::now();

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.green} [{elapsed_precise}] {pos} users processed {msg}").unwrap()
    );

    let mut db_streaming = SqliteAdapter::new("./output-streaming.db")?;

    let multi_reader = multi_extract_streaming(&paths, 1000)?;

    let stats = multi_reader
        .transform(generate_user)
        .filter(|user| user.is_valid().is_ok())
        .load(|users| {
            spinner.inc(users.len() as u64);
            spinner.set_message(format!("Loading... just added {}", users.len()));
            db_streaming.write(users)?;
            Ok(())
        })?;

    spinner.finish_with_message("Done!");

    println!("Streaming with DB: {:?}", time_start.elapsed());
    println!("Stat Streaming : {:?}", stats);

    Ok(())
}
