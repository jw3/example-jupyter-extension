use std::env::args;
use std::error::Error;
use meos::prelude::{Temporal, TInst};
use polars::datatypes::AnyValue::{Int64, List, UInt32};
use pyo3::prelude::*;
use polars::prelude::*;

#[pymodule]
#[pyo3(name="keplerviz")]
fn keplerviz_module(_py: Python, m: &PyModule) -> PyResult<()> {
    meos::init();
    m.add_function(wrap_pyfunction!(load_ais_csv, m)?)?;
    Ok(())
}

#[pyfunction]
fn load_ais_csv(f: &str) -> PyResult<()> {
    println!("{f}");
    let df = LazyCsvReader::new(f).has_header(true).finish().expect("finish");

    let df = df
        .select([
            col("MMSI"),
            col("BaseDateTime").alias("T"),
            col("LAT"),
            col("LON"),
        ])
        .group_by(["MMSI"])
        .agg([
            len(),
            col("T").sort(SortOptions::default().with_order_descending(false)),
            concat_str([col("LON"), col("LAT")], " ", true).alias("P"),
        ])
        .limit(1)
        .collect().expect("lazy");
    let sz = df.height();
    if let [m, l, t, p] = df.get_columns() {
        for i in 0..sz {
            let mut metric_trip_sz = 0;
            match (m.get(i).unwrap(), l.get(i).unwrap(), t.get(i).unwrap(), p.get(i).unwrap()) {
                (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                    println!("{mmsi} {len}");
                    to_posit(&pt, &ts).iter().for_each(|p| {
                        println!("\t{}", p.to_mf_json().unwrap());
                    });
                }
                _ => {}
            }
        }
    };
    Ok(())
}

fn to_posit(p: &Series, t: &Series) -> Vec<TInst> {
    p.iter()
        .zip(t.iter())
        .map(|(p, t)| {
            let p = p.get_str().unwrap();
            let t = t.get_str().unwrap();
            TInst::from_wkt(&format!("SRID=4326;Point({p})@{t}+00")).expect("tinst")
        })
        .collect()
}
