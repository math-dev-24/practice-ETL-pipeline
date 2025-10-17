use std::fs;
use crate::models::recipe_config::RecipeConfig;

pub fn parse_yaml(path: &str) -> Result<RecipeConfig, serde_yaml::Error> {
    let yam_content = fs::read_to_string(path).unwrap();
    let config: RecipeConfig = serde_yaml::from_str(&yam_content)?;
    Ok(config)
}

