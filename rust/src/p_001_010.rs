use polars::lazy::dsl::{col, lit, when};

use crate::core::{read_csv, ORG_PATH};

pub fn p_001() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path).collect().unwrap().head(Some(10));
    println!("{:?}", df);
}

pub fn p_002() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path)
        .select(&[
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));
    println!("{:?}", df);
}

pub fn p_003() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path)
        .select(&[
            col("sales_ymd").alias("sales_ymd_date"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));
    println!("{:?}", df);
}

pub fn p_004() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path)
        .filter(col("customer_id").str().contains("CS018205000001"))
        .select(&[
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .collect()
        .unwrap();
    println!("{:?}", df);
}

pub fn p_005() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path)
        .filter(col("customer_id").str().contains("CS018205000001"))
        .filter(col("amount").gt(lit(1000)))
        .select(&[
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .collect()
        .unwrap();
    println!("{:?}", df);
}

pub fn p_006() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path)
        .filter(col("customer_id").str().contains("CS018205000001"))
        .filter(col("amount").gt_eq(1000).or(col("quantity").gt_eq(5)))
        .select(&[
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("quantity"),
            col("amount"),
        ])
        .collect()
        .unwrap();
    println!("{:?}", df);
}

pub fn p_007() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path)
        .filter(col("customer_id").str().contains("CS018205000001"))
        .filter(col("amount").gt_eq(1000))
        .filter(col("amount").lt_eq(2000))
        .select(&[
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .collect()
        .unwrap();
    println!("{:?}", df);
}

pub fn p_008() {
    let path = ORG_PATH.to_string() + "receipt.csv";
    let df = read_csv(&path)
        .filter(col("customer_id").str().contains("CS018205000001"))
        .with_column(
            when(col("product_cd").str().contains("P071401019"))
                .then(1)
                .otherwise(0)
                .alias("isExist"),
        )
        .filter(col("isExist").eq(0))
        .select(&[
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .collect()
        .unwrap();
    println!("{:?}", df);
}

pub fn p_009() {
    let path = ORG_PATH.to_string() + "store.csv";
    let df = read_csv(&path)
        .filter(col("prefecture_cd").neq(13))
        .filter(col("floor_area").lt_eq(900))
        .collect()
        .unwrap();
    println!("{:?}", df);
}

pub fn p_010() {
    let path = ORG_PATH.to_string() + "store.csv";
    let df = read_csv(&path)
        .filter(col("store_cd").str().starts_with("S14"))
        .collect()
        .unwrap()
        .head(Some(10));
    println!("{:?}", df);
}
