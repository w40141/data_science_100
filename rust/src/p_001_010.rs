use crate::core::{read_csv, ORG_PATH};

pub fn p_001() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path).collect().unwrap().head(Some(10));
    println!("{:?}", df);
}
