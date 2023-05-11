use std::{
    fs::{File, read_dir},
    io::{prelude::*, BufReader},
    path::Path
};

use csv::{WriterBuilder, ReaderBuilder};
use pyo3::{prelude::*, wrap_pyfunction};


#[pyfunction]
pub fn filter_row(csv_path: &str, output_path: &str, sep: u8, col: usize, cond: &str, is_exac: bool) -> PyResult<()> {
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(sep)
        .from_path(csv_path)
        .unwrap();

    let mut csv_writer = WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)
        .unwrap();

    let headers = csv_reader.headers().unwrap().clone();
    csv_writer.write_record(&headers).unwrap();

    if is_exac {
        for result in csv_reader.records() {
            let record  = result.unwrap();
            if record.get(col) == Some(cond) {
                csv_writer.write_record(&record).unwrap();
            }
        }
    } else {
        for result in csv_reader.records() {
            let record = result.unwrap();
            if record.get(col).unwrap().contains(cond) {
                csv_writer.write_record(&record).unwrap();
            }
        }
    }
    csv_writer.flush()?;
    Ok(())
}

#[pyfunction]
pub fn filter_rows(txt_path: &str, csv_path: &str, output_path: &str, sep: u8, col: usize) -> PyResult<()> {
    let mut txt = File::open(txt_path)?;
    let mut contents = String::new();
    txt.read_to_string(&mut contents)?;
    let mut arr: Vec<String> = Vec::new();
    for line in contents.lines() {
        arr.push(line.to_string());
    }

    let mut csv_writer = WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)
        .unwrap();

    let csv_file = File::open(csv_path)?;
    let reader = BufReader::new(csv_file);
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(sep)
        .from_reader(reader);

    let headers = csv_reader.headers().unwrap().clone();
    csv_writer.write_record(&headers).unwrap();

    for result in csv_reader.records() {
        let record = result.unwrap();
        if record.get(col).map_or(false, |s| arr.iter().any(|a| s.contains(a))) {
            csv_writer.write_record(&record).unwrap();
        }
    }
    
    csv_writer.flush()?;
    Ok(())
}

#[pyfunction]
pub fn merge_csv(folder_path: &str, output_path: &str, sep: u8) -> PyResult<()> {
    let dir_path =  Path::new(folder_path);
    let out_path = Path::new(output_path);
    let mut csv_writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(out_path)
        .unwrap();
    let mut header_written = false;
    for entry in read_dir(dir_path)? {
        let file_path = entry?.path();
        if file_path.is_file() && file_path.extension().unwrap_or_default() == "csv" {
            let mut csv_reader = ReaderBuilder::new()
                .has_headers(true)
                .delimiter(sep)
                .from_path(file_path)
                .unwrap();

            if !header_written {
                let header = csv_reader.headers().unwrap().clone();
                csv_writer.write_record(header.iter()).unwrap();
                header_written = true;
            }

            for result in csv_reader.records() {
                let record = result.unwrap();
                csv_writer.write_record(record.iter()).unwrap();
            }
        }
    }
    csv_writer.flush()?;
    Ok(())
}

#[pyfunction]
pub fn split_csv(csv_path: &str, save_path: &str, sep: u8, row_cnt: i32) -> PyResult<()> {
    let file_path = Path::new(csv_path);
    let file_name = file_path.file_stem().unwrap().to_str().unwrap();

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(sep)
        .from_path(csv_path).unwrap();

    let mut csv_writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(format!("{}/{}_1.csv", save_path, file_name)).unwrap();

    let headers = csv_reader.headers().unwrap().clone();
    csv_writer.write_record(&headers).unwrap();

    let mut row_count = 0;
    let mut file_count = 1;

    for result in csv_reader.records() {
        let record = result.unwrap();
        csv_writer.write_record(&record).unwrap();

        row_count += 1;

        if row_count == row_cnt {
            csv_writer.flush()?;
            row_count = 0;
            file_count += 1;
            csv_writer = WriterBuilder::new()
                .has_headers(true)
                .from_path(format!("{}/{}_{}.csv", save_path, file_name, file_count)).unwrap();
            csv_writer.write_record(&headers).unwrap();
        }
    }
    csv_writer.flush()?;
    Ok(())
}


#[pymodule]
fn pycrab(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(filter_row, m)?)?;
    m.add_function(wrap_pyfunction!(filter_rows, m)?)?;
    m.add_function(wrap_pyfunction!(merge_csv, m)?)?;
    m.add_function(wrap_pyfunction!(split_csv, m)?)?;
    Ok(())
}