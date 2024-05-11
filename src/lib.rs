use std::error::Error;

use chrono::DateTime;
use keplerize::{Data, Dataset, Feature, Info, LineString, Row};
use meos::prelude::{Temporal, TInst, TSeq};
use polars::datatypes::AnyValue::{Int64, List, UInt32};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::{PyDataFrame, PyLazyFrame};
use serde::{Deserialize, Deserializer, Serialize};

mod render;

#[derive(Deserialize, Debug)]
struct Rec {
    pub id: u64,
    pub vt: u32,
    pub json: Mf,
}

#[derive(Deserialize, Debug)]
struct Mf {
    pub coordinates: Vec<[f64; 2]>,

    #[serde(deserialize_with = "str_to_ts")]
    pub datetimes: Vec<i64>,
}

#[derive(Deserialize, Serialize, Debug)]
struct MyRow(Feature, u64, u32);

#[typetag::serde]
impl Row for MyRow {}

impl From<Rec> for MyRow {
    fn from(src: Rec) -> Self {
        assert_eq!(src.json.coordinates.len(), src.json.datetimes.len());
        let coords = src
            .json
            .datetimes
            .into_iter()
            .map(|t| t as f64)
            .zip(src.json.coordinates)
            .into_iter()
            .map(|(t, [x, y])| [x, y, 0.0, t]);
        let g = LineString {
            //geometry_type: "LineString",
            coordinates: coords.collect(),
        };
        MyRow(Feature { geometry: g }, src.id, src.vt)
    }
}

fn str_to_ts<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<i64>, D::Error> {
    let s: Vec<&str> = Deserialize::deserialize(d)?;
    let r: Vec<_> = s
        .iter()
        .flat_map(|x| DateTime::parse_from_str(x, "%Y-%m-%dT%T%#z"))
        .map(|x| x.timestamp())
        .collect();

    if s.len() == r.len() {
        Ok(r)
    } else {
        Err(serde::de::Error::custom(format!(
            "lossy ts convert: {} to {}",
            s.len(),
            r.len()
        )))
    }
}

#[pyfunction]
pub fn load_ais_csv(path: &str) -> PyHtml {
    //let x = env::var("KEPLER_SIZE").map(|v|v.split_once(",").map(|(x, y)|(x.parse(), y.parse())).unwrap_or(("x", "y"))).unwrap_or(("x", "y"));

    println!("load csv {path}");
    let df = LazyCsvReader::new(path).has_header(true).finish().expect("finish");
    let df = df.select([
        col("MMSI"),
        col("BaseDateTime").alias("T"),
        col("LAT"),
        col("LON"),
    ]);
    keplerize_lazy_frame(df)
}

#[pyfunction]
pub fn keplerize_df(df: PyDataFrame) -> crate::PyHtml {
    let df :DataFrame  = df.into();
    keplerize_lazy_frame(df.lazy())
}

#[pyfunction]
pub fn keplerize_lf(df: PyLazyFrame) -> PyHtml {
    keplerize_lazy_frame(df.into())
}

#[pyfunction]
pub fn a(df: PyLazyFrame) -> PyDataFrame {
    let df: LazyFrame = df.into();
    let df = df
        .group_by(["mmsi"])
        .agg([
            len(),
            col("t").sort(SortOptions::default()),
            concat_str([col("lon"), col("lat")], " ", true).alias("p"),
        ]).filter(col("len").gt(1))
        .collect().expect("lazy");
    PyDataFrame(df)
}

#[pyfunction]
pub fn b1(df: PyDataFrame) -> String {
    let df: DataFrame = df.into();
    format!("{}", df.height())
}

#[pyfunction]
pub fn b2(df: PyDataFrame) -> String {
    let df: DataFrame = df.into();
    let sz = df.height();
    let mut rows = vec![];
    if let [m, l, t, p] = df.get_columns() {
        let vtype = 0;
        for i in 0..sz {
            match (m.get(i).expect("m"), l.get(i).expect("l"), t.get(i).expect("t"), p.get(i).expect("p")) {
                (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                    let ulen = ts.unique().iter().len();
                    if ulen as u32 != len {
                        rows.push(format!("{mmsi} {ulen} != {len} on ts"))
                    }
                    // if TSeq::make(&to_posit(&pt, &ts)).is_none() {
                    //     rows.push(format!("{mmsi}"))
                    // }
                }
                _ => {
                    match m.get(i).expect("m2") {
                        Int64(mmsi) => rows.push("m-OK".to_string()),
                        x => rows.push(format!("m-FAIL {x:?}")),
                    }
                    match l.get(i).expect("l2") {
                        UInt32(len) => rows.push("l-OK".to_string()),
                        x => rows.push(format!("l-FAIL {x:?}")),
                    }
                    match t.get(i).expect("t2") {
                        List(ts) => rows.push("t-OK".to_string()),
                        x => rows.push(format!("t-FAIL {x:?}")),
                    }
                    match p.get(i).expect("p2") {
                        List(pt) => rows.push("p-OK".to_string()),
                        x => rows.push(format!("p-FAIL {x:?}")),
                    }
                    // return format!("missed on {i}")
                    break;
                }
            }
        }
    };
    rows.join(",")
}

#[pyfunction]
pub fn b(df: PyDataFrame) -> String {
    let df: DataFrame = df.into();
    let sz = df.height();
    let mut rows = vec![];
    if let [m, l, t, p] = df.get_columns() {
        let vtype = 0;
        for i in 0..sz {
            match (m.get(i).expect("m"), l.get(i).expect("l"), t.get(i).expect("t"), p.get(i).expect("p")) {
                (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                    let seq = TSeq::make(&to_posit(&pt, &ts)).expect("tseq");
                    let output = seq.as_json().unwrap();
                    let ser = format!(r#"{{"id":{mmsi},"vt":{vtype},"json":{output}}}"#);
                    let de = serde_json::from_str::<Rec>(&ser).unwrap();
                    rows.push(MyRow::from(de));
                }
                _ => {
                    return format!("missed on {i}")
                }
            }
        }
    };
    "ok".to_owned()
}

#[pyfunction]
pub fn ab(df: PyLazyFrame) -> String {
    let df: LazyFrame = df.into();
    let df = df.group_by(["mmsi"])
        .agg([
            len(),
            col("t").sort(SortOptions::default()),
            concat_str([col("lon"), col("lat")], " ", true).alias("p"),
        ])
        .collect().expect("lazy");
    let df: DataFrame = df.into();
    let sz = df.height();
    let mut rows = vec![];
    if let [m, l, t, p] = df.get_columns() {
        let vtype = 0;
        for i in 0..sz {
            match (m.get(i).expect("m"), l.get(i).expect("l"), t.get(i).expect("t"), p.get(i).expect("p")) {
                (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                    let seq = TSeq::make(&to_posit(&pt, &ts)).expect("tseq");
                    let output = seq.as_json().unwrap();
                    let ser = format!(r#"{{"id":{mmsi},"vt":{vtype},"json":{output}}}"#);
                    let de = serde_json::from_str::<Rec>(&ser).unwrap();
                    rows.push(MyRow::from(de));
                }
                _ => {
                    return format!("missed on {i}")
                }
            }
        }
    };
    "ok".to_owned()
}

#[pyfunction]
pub fn c(df: PyLazyFrame) -> String {
    let df: LazyFrame = df.into();
    let df = df.group_by(["mmsi"])
        .agg([
            len(),
            col("t").sort(SortOptions::default()),
            concat_str([col("lon"), col("lat")], " ", true).alias("p"),
        ])
        .collect().expect("lazy");
    let sz = df.height();
    let mut rows = vec![];
    if let [m, l, t, p] = df.get_columns() {
        let vtype = 0;
        for i in 0..sz {
            match (m.get(i).unwrap(), l.get(i).unwrap(), t.get(i).unwrap(), p.get(i).unwrap()) {
                (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                    let seq = TSeq::make(&to_posit(&pt, &ts)).expect("tseq");
                    let output = seq.as_json().unwrap();
                    let ser = format!(r#"{{"id":{mmsi},"vt":{vtype},"json":{output}}}"#);
                    let de = serde_json::from_str::<Rec>(&ser).unwrap();
                    rows.push(MyRow::from(de));
                }
                _ => {}
            }
        }
    };
    let ds = Dataset::<MyRow> {
        info: Info {
            id: "0000",
            label: "example",
        },
        data: Data {
            fields: &["id".into(), "vessel-type".into()],
            rows: &rows,
        },
    };
    "ok".to_owned()
}


pub fn keplerize_lazy_frame(df: LazyFrame) -> PyHtml {
    let df: LazyFrame = df.into();
    let df = df
        .group_by(["mmsi"])
        .agg([
            len(),
            col("t").sort(SortOptions::default()),
            concat_str([col("lon"), col("lat")], " ", true).alias("p"),
        ]).filter(col("len").gt(1))
        .collect().expect("lazy");

    let sz = df.height();
    let mut rows = vec![];
    if let [m, l, t, p] = df.get_columns() {
        let vtype = 0;
        for i in 0..sz {
            match (m.get(i).unwrap(), l.get(i).unwrap(), t.get(i).unwrap(), p.get(i).unwrap()) {
                (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                    let seq = TSeq::make(&to_posit(&pt, &ts)).expect("tseq");
                    let output = seq.as_json().unwrap();
                    let ser = format!(r#"{{"id":{mmsi},"vt":{vtype},"json":{output}}}"#);
                    let de = serde_json::from_str::<Rec>(&ser).unwrap();
                    rows.push(MyRow::from(de));
                }
                _ => {}
            }
        }
    };

    let ds = Dataset::<MyRow> {
        info: Info {
            id: "0000",
            label: "example",
        },
        data: Data {
            fields: &["id".into(), "vessel-type".into()],
            rows: &rows,
        },
    };
    render::map(ds)
}

#[pyclass]
struct PyHtml {
    html: String,
}

#[pymethods]
impl PyHtml {
    fn _repr_html_(&self) -> String {
        format!("<iframe width='75%' height='400px' srcdoc=\"{}\"></iframe>", self.html.replace('\n', ""))
    }
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


#[pymodule]
#[pyo3(name = "keplerviz")]
fn keplerviz_module(_py: Python, m: &PyModule) -> PyResult<()> {
    meos::init();
    m.add_function(wrap_pyfunction!(a, m)?)?;
    m.add_function(wrap_pyfunction!(ab, m)?)?;
    m.add_function(wrap_pyfunction!(b, m)?)?;
    m.add_function(wrap_pyfunction!(b2, m)?)?;
    m.add_function(wrap_pyfunction!(c, m)?)?;
    m.add_function(wrap_pyfunction!(load_ais_csv, m)?)?;
    m.add_function(wrap_pyfunction!(keplerize_df, m)?)?;
    m.add_function(wrap_pyfunction!(keplerize_lf, m)?)?;

    m.add_class::<PyHtml>()?;
    Ok(())
}
