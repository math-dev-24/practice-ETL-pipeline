use serde::de::Error;
use crate::models::pipeline::Pipeline;
use crate::models::user::User;
use crate::utils::set_user::generate_user;

#[derive(Debug)]
pub enum TransformFn {
    GenerateUser,
    Capitalize,
    Lowercase
}

pub enum FilterFn {
    IsValid
}

impl TransformFn {
    pub fn from_str(name: &str) -> Option<TransformFn> {
        match name {
            "generate_user" => Some(TransformFn::GenerateUser),
            "capitalize" => Some(TransformFn::Capitalize),
            "lowercase" => Some(TransformFn::Lowercase),
            _ => None
        }
    }

    pub fn apply_to_csv(self, pipeline: Pipeline<csv::StringRecord> ) -> Pipeline<User> {
        match self {
            TransformFn::GenerateUser => pipeline.transform(generate_user),
            _ => panic!("Cette transformation ne marche que sur GenerateUser")
        }
    }

    pub fn apply_to_user(self, pipeline: Pipeline<User>) -> Pipeline<User> {
        match self {
            TransformFn::Capitalize => {
                pipeline.transform(|mut user| {
                    user.first_name = user.first_name.to_uppercase();
                    user
                })
            },
            TransformFn::Lowercase => {
                pipeline.transform(|mut user| {
                    user.first_name = user.first_name.to_lowercase();
                    user
                })
            },
            _ => panic!("Cette transformation ne marche pas sur des Users!")
        }
    }
    
}

impl FilterFn {
    pub fn from_str(name: &str) -> Option<FilterFn> {
        match name {
            "is_valid" => Some(FilterFn::IsValid),
            _ => None
        }
    }
    pub fn apply_to_user(self, pipeline: Pipeline<User>) -> Pipeline<User> {
        match self {
            FilterFn::IsValid => {
                pipeline.filter(|user| user.is_valid().is_ok())
            },
            _ => panic!("Ce filtre ne marche que sur IsValid")
        }
    }
}

