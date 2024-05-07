use keplerize::{Dataset, Row};
use serde::Serialize;
use crate::PyHtml;

pub fn map<T: Row + Serialize>(ds: Dataset<T>) -> PyHtml {
    let serialized = serde_json::to_string(&ds).unwrap();
    let s = include_str!("template.html");
    let html_content = s.replacen("{<!--__DATASETS__-->}", &serialized, 1);

    let escaped_html_content = html_content.replace("\"", "&quot;");

    PyHtml{
        html: escaped_html_content
    }
}
