use rust_bert::pipelines::sentence_embeddings::{
    SentenceEmbeddingsBuilder, SentenceEmbeddingsModelType,
};

#[derive(Debug)]
pub struct Vectorizer {
    model: SentenceEmbeddingsModelType,
    indices: Vec<u32>,
}

impl Vectorizer {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let model =
            SentenceEmbeddingsBuilder::new(SentenceEmbeddingsModelType::SentenceGTEbase).build()?;
        let indices = Vec::new();

        Ok(Vectorizer { model, indices })
    }

    pub fn embed(&self, query: &str) -> Vec<u8> {
        println!("Querying: {}", query);
        let embeddings = self.model.encode(&[query])?;
        Ok((embeddings))
    }
}
