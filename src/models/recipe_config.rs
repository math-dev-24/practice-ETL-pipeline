use serde::Deserialize;
use crate::models::pipeline::Pipeline;
use crate::models::registry::{FilterFn, TransformFn};
use crate::models::user::User;
use crate::utils::multi_extract::multi_extract;

#[derive(Debug, Deserialize)]
pub struct RecipeConfig {
    pub name: String,
    pub source: SourceConfig,
    pub steps: Vec<StepConfig>,
    pub output: OutputConfig,

}


#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FormatFile {
    CSV,
    JSON,
    SQLITE,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    pub format: FormatFile,
    pub path: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct StepConfig {
    pub action: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct OutputConfig {
    pub format: FormatFile,
    pub path: String,
}

impl RecipeConfig {
    pub fn execute(&self) -> Result<Pipeline<User>, Box<dyn std::error::Error>> {
        let paths: Vec<&str> = self.source.path.iter().map(|p| p.as_str()).collect();
        let current_pipeline = multi_extract(&paths)?;

        let first_transform = self.steps.first().ok_or("Pas de transformation")?;
        let transform_fn = TransformFn::from_str(first_transform.value.as_str())
            .ok_or_else(|| format!("Transformation inconnue: {}", first_transform.value))?;

        let mut user_pipeline = transform_fn.apply_to_csv(current_pipeline);

        for step in self.steps.iter().skip(1) {
            user_pipeline = execute_step(step, user_pipeline);
        }

        Ok(user_pipeline)
    }


}

fn execute_step(step: &StepConfig, pipeline: Pipeline<User>) -> Pipeline<User> {
    match step.action.as_str() {
        "transform" => {
            if let Some(t) = TransformFn::from_str(&step.value) {
                t.apply_to_user(pipeline)
            } else {
                pipeline
            }
        },
        "filter" => {
            if let Some(f) = FilterFn::from_str(&step.value) {
                f.apply_to_user(pipeline)
            } else {
                pipeline
            }
        },
        _ => pipeline
    }
}