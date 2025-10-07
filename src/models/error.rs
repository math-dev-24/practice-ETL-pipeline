
pub type ValidationResult = Result<(), Vec<ValidationError>>;

#[derive(Debug, Clone)]
pub enum ValidationError {
    EmptyField(String),
    InvalidFormat(String, String),
    TooShort(String, usize),
    TooLong(String, usize),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ValidationError::EmptyField(field) => write!(f, "{} cannot be empty", field),
            ValidationError::InvalidFormat(field, expected) => write!(f, "{} has invalid format (expected: {})", field, expected),
            ValidationError::TooShort(field, min) => write!(f, "{} too short (minimum: {} chars)", field, min),
            ValidationError::TooLong(field, max) => write!(f, "{} too long (maximum: {} chars)", field, max),
        }
    }
}