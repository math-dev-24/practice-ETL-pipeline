mod storage;
mod models;
mod utils;

use std::sync::{Arc, Mutex};
use crate::models::error::ValidationError;
use crate::storage::init::Database;
use crate::models::user::User;
use crate::models::pipeline::{Pipeline};


fn main() -> Result<(), Box<dyn std::error::Error>> {

    let errors: Arc<Mutex<Vec<ValidationError>>> = Arc::new(Mutex::new(Vec::new()));

    let pipeline = Pipeline::extract("./src/data/username.csv")?
        .transform(|record| {
            User {
                username: record.get(0).unwrap_or("").to_string(),
                identifier: record.get(1).unwrap_or("").to_string(),
                first_name: record.get(2).unwrap_or("").to_string(),
                last_name: record.get(3).unwrap_or("").to_string(),
            }
        })
        .filter(|user| {
            match user.is_valid() {
                Ok(_) => true,
                Err(err) => {
                    println!("{:?}", err);
                    errors.lock().unwrap().extend(err);
                    false
                },
            }
        });

    pipeline.report();

    let validation_errors = errors.lock().unwrap();
    println!("Total validation errors: {}", validation_errors.len());
    drop(validation_errors);
    
    let db = Database::new("./output.db");
    db.init()?;
    db.insert_user(&pipeline.data)?;

    let users_db = db.get_all_users()?;
    println!("{:#?}", users_db);

    Ok(())
}
