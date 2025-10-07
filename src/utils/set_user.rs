use csv::StringRecord;
use crate::models::user::User;

pub fn generate_user(record: StringRecord) -> User {
    User {
        username: record.get(0).unwrap_or("").to_string(),
        identifier: record.get(1).unwrap_or("").to_string(),
        first_name: record.get(2).unwrap_or("").to_string(),
        last_name: record.get(3).unwrap_or("").to_string(),
    }
}