use std::error::Error;
use crate::models::csv_multi_reader::MultiCsvReader;
use crate::models::pipeline::{Pipeline, PipelineStats};
use crate::models::stream_pipeline::StreamingPipeline;

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


pub fn multi_extract_streaming(sources: &[&str], chunk_size: usize)
-> Result<StreamingPipeline<MultiCsvReader, csv::StringRecord>, Box<dyn Error>> {
    if sources.is_empty() {
        return Err("No sources provided".into());
    }

    let multi_reader = MultiCsvReader::new(&sources, chunk_size)?;

    Ok(StreamingPipeline {
        chunks: multi_reader,
        stats: PipelineStats::default()
    })
}