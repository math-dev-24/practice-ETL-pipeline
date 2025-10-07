use crate::models::error::{ValidationError, ValidationResult};

#[derive(Debug)]
pub struct User {
    pub username: String,
    pub identifier: String,
    pub first_name: String,
    pub last_name: String,
}

impl User {
    pub fn is_valid(&self) -> ValidationResult {
        let mut errors = Vec::new();

        if self.username.is_empty() {
            errors.push(ValidationError::EmptyField("username".into()));
        } else if self.username.len() < 3 {
            errors.push(ValidationError::TooShort("username".into(), 2));
        } else if self.username.len() > 20 {
            errors.push(ValidationError::TooLong("username".into(), 20));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}