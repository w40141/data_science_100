use polars::prelude::{LazyCsvReader, LazyFrame};

pub static ORG_PATH: &str = "../../../The-Japan-DataScientist-Society/100knocks-preprocess/docker/work/data/";

pub fn read_csv(path: &str) -> LazyFrame {
    LazyCsvReader::new(path)
        .has_header(true)
        .finish().unwrap()
}
