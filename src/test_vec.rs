
use minisearch::vectorizer::Vectorizer;

fn main() {
    let vectorizer = Vectorizer::new().unwrap();
    let query = "hello world";
    let embeddings = vectorizer.embed(query).unwrap();
    println!("Embeddings: {:?}", embeddings);
}