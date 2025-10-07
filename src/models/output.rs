use std::error::Error;

pub trait OutputPort<T> {
    fn write(&mut self, data: &[T]) -> Result<(), Box<dyn Error>>;
    fn finalize(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}