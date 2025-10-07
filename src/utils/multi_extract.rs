use std::error::Error;
use crate::models::pipeline::Pipeline;

pub fn multi_extract(sources: &[&str]) -> Result<Pipeline<csv::StringRecord>, Box<dyn Error>> {

    if sources.is_empty() {
        return Err("No sources provided".into());
    }

    let mut pipelines = sources.iter()
        .map(|source| Pipeline::extract(source))
        .collect::<Result<Vec<_>, _>>()?;

    let mut result = pipelines.remove(0);

    for pipeline in pipelines {
        result = result.merge(pipeline);
    }

    Ok(result)

}