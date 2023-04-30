use std::{
    fs::{File, read_dir},
    io::{prelude::*, BufReader},
    path::Path,
    error::Error
};

use sqlx::{MySqlPool, query, Row};
use mysql::prelude::Queryable;
use indicatif::ProgressBar;
use csv::{WriterBuilder, ReaderBuilder, Writer, Reader};
use pyo3::{prelude::*, wrap_pyfunction};


#[pyfunction]
pub fn filter_row(csv_path: &str, output_path: &str, sep: u8, col: usize, cond: &str) -> PyResult<()> {
    // open the input CSV file
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(sep)
        .from_path(csv_path)
        .unwrap();

    // open the output CSV file
    let mut csv_writer = WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)
        .unwrap();

    // read headers
    let headers = csv_reader.headers().unwrap().clone();
    csv_writer.write_record(&headers).unwrap();

    // iterate over each record in the input CSV file
    for result in csv_reader.records() {
        let record = result.unwrap();
        // check if the col field contains the cond
        if record.get(col).unwrap().contains(cond) {
            // write the record to the output CSV file
            csv_writer.write_record(&record).unwrap();
        }
    }

    // flush the output CSV file to disk
    csv_writer.flush()?;
    Ok(())
    }

#[pyfunction]
pub fn filter_rows(txt_path: &str, csv_path: &str, output_path: &str, sep: u8, col: usize) -> PyResult<()> {
    /*
        Multiple filtering criteria -> 1 CSV file
    */

    // read txt file
    let mut txt = File::open(txt_path)?;
    let mut contents = String::new();
    txt.read_to_string(&mut contents)?;
    let mut arr: Vec<String> = Vec::new();
    for line in contents.lines() {
        arr.push(line.to_string());
    }

    // define writer
    let mut csv_writer = WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)
        .unwrap();

    // read csv file
    let csv_file = File::open(csv_path)?;
    let reader = BufReader::new(csv_file);
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(sep)
        .from_reader(reader);

    // read headers
    let headers = csv_reader.headers().unwrap().clone();
    csv_writer.write_record(&headers).unwrap();

    // progressbar
    let pb = ProgressBar::new_spinner();

    // filter
    for result in csv_reader.records() {
        let record = result.unwrap();
        if record.get(col).map_or(false, |s| arr.iter().any(|a| s.contains(a))) {
            csv_writer.write_record(&record).unwrap();
        }
        pb.inc(1);
    }
    pb.finish_with_message("done.");
    csv_writer.flush()?;
    Ok(())
}

#[pyfunction]
pub fn merge_csv(folder_path: &str, output_path: &str) -> PyResult<()> {
    let dir_path =  Path::new(folder_path);
    let out_path = Path::new(output_path);
    let mut csv_writer = Writer::from_path(out_path).unwrap();
    let mut header_written = false;
    for entry in read_dir(dir_path)? {
        let file_path = entry?.path();
        if file_path.is_file() && file_path.extension().unwrap_or_default() == "csv" {
            let mut reader = Reader::from_path(&file_path).unwrap();
            if !header_written {
                let header = reader.headers().unwrap().clone();
                csv_writer.write_record(header.iter()).unwrap();
                header_written = true;
            }
            for result in reader.records() {
                let record = result.unwrap();
                csv_writer.write_record(record.iter()).unwrap();
            }
        }
    }
    csv_writer.flush()?;
    Ok(())
}

async fn conn_mysql(url: &str, url_sql: &str, company_name: &str, save_path: &str) -> Result<(), Box<dyn Error>> {
    let pool_q_code = mysql::Pool::new(url).unwrap();
    let mut conn = pool_q_code.get_conn().unwrap();
    let query_sql = format!("SELECT DbName FROM deloitte.b_projectlist WHERE ProjectName = '{}'", company_name);

    let unique_code: Option<String> = conn.exec_first(query_sql, ()).unwrap();
    let get_code_str = unique_code.ok_or("No code found").unwrap();

    let pool: sqlx::Pool<sqlx::MySql> = MySqlPool::connect(&url_sql).await.unwrap();
    let sql_len_gl = format!("SELECT COUNT(被审计单位) AS length FROM {}.凭证表", get_code_str);
    let len_gl = query(&sql_len_gl).fetch_all(&pool).await.unwrap();
    let mut len_gl_vec = Vec::new();
    for row in len_gl {
        let get_len_gl: i32 = row.get("length");
        len_gl_vec.push(get_len_gl)
    }
    let mut start = 0;
    let stop = len_gl_vec[0];
    let step = 300_0000;
    let mut file_count = 1;
    let mut split_filename = company_name.split("_");
    let filename = split_filename.nth(2).unwrap_or(company_name);
    println!("{} - rows => {:?}", filename, len_gl_vec[0]);

    // query gl
    for _ in (start..=stop).step_by(step) {
        let sql_query_gl = format!(
            "SELECT 被审计单位, 审计期间, 记账时间, 会计年, 会计月, 凭证种类, 凭证编号, 分录号, 业务说明, 币种, 汇率, 科目编号,
                上级科目名称, 科目名称, 核算项目名称, CAST(借方发生额 AS CHAR) AS 借方发生额, CAST(贷方发生额 AS CHAR) AS 贷方发生额,
                CAST(`借方发生额-外币` AS CHAR) AS `借方发生额-外币`, CAST(`贷方发生额-外币` AS CHAR) AS `贷方发生额-外币`,
                CAST(借方数量 AS CHAR) AS 借方数量, CAST(贷方数量 AS CHAR) AS 贷方数量, 制单人, 制单日期, 审核人, 审核日期, 
                备注01, 备注02, 备注03, 备注04, 备注05, 备注06, 备注07,
                备注08, 备注09, 备注10, 备注11, 备注12, 备注13, 备注14, 备注15, 备注16, 备注17, 备注18, 备注19, 备注20, 备注21,
                备注22, 备注23, 备注24, 备注25, 备注26, 备注27, 备注28, 备注29, 备注30
            FROM {}.凭证表
            LIMIT {}, {}", get_code_str, start, step);
        let data_gl = query(&sql_query_gl).fetch_all(&pool).await?;
        let step_i32: i32 = step as i32;
        let output_path_single = format!("{}/{}_GL.csv", save_path, filename);
        let output_path_multi = format!("{}/{}_GL_{}.csv", save_path, filename, file_count);
        let output_path = if step_i32 > stop { output_path_single } else { output_path_multi };
        let mut csv_writer_gl = WriterBuilder::new()
            .delimiter(b'|')
            .from_path(output_path).unwrap();
        csv_writer_gl.write_record(&["被审计单位", "审计期间", "记账时间", "会计年", "会计月", "凭证种类", "凭证编号", "分录号", "业务说明", "币种",
                        "汇率", "科目编号", "上级科目名称", "科目名称", "核算项目名称", "借方发生额", "贷方发生额", "借方发生额-外币",
                        "贷方发生额-外币", "借方数量", "贷方数量", "制单人", "制单日期", "审核人", "审核日期", "备注01", "备注02",
                        "备注03", "备注04", "备注05", "备注06", "备注07", "备注08", "备注09", "备注10", "备注11", "备注12", "备注13",
                        "备注14", "备注15", "备注16", "备注17", "备注18", "备注19", "备注20", "备注21", "备注22", "备注23", "备注24",
                        "备注25", "备注26", "备注27", "备注28", "备注29", "备注30"])?;
        for data in data_gl {
            let a1: &str = data.get("被审计单位");
            let b1: &str = data.get("审计期间");
            let c1: &str = data.get("记账时间");
            let d1: i32 = data.get("会计年");
            let e1: i32 = data.get("会计月");
            let f1: &str = data.get("凭证种类");
            let g1: &str = data.get("凭证编号");
            let h1: &str = data.get("分录号");
            let i1: &str = data.get("业务说明");
            let j1: &str = data.get("币种");
            let k1: &str = data.get("汇率");
            let l1: &str = data.get("科目编号");
            let m1: &str = data.get("上级科目名称");
            let n1: &str = data.get("科目名称");
            let o1: &str = data.get("核算项目名称");
            let p1: &str = data.get("借方发生额");
            let q1: &str = data.get("贷方发生额");
            let r1: &str = data.get("借方发生额-外币");
            let s1: &str = data.get("贷方发生额-外币");
            let t1: &str = data.get("借方数量");
            let u1: &str = data.get("贷方数量");
            let v1: &str = data.get("制单人");
            let w1: &str = data.get("制单日期");
            let x1: &str = data.get("审核人");
            let y1: &str = data.get("审核日期");
            let z1: &str = data.get("备注01");
            let aa1: &str = data.get("备注02");
            let ab1: &str = data.get("备注03");
            let ac1: &str = data.get("备注04");
            let ad1: &str = data.get("备注05");
            let ae1: &str = data.get("备注06");
            let af1: &str = data.get("备注07");
            let ag1: &str = data.get("备注08");
            let ah1: &str = data.get("备注09");
            let ai1: &str = data.get("备注10");
            let aj1: &str = data.get("备注11");
            let ak1: &str = data.get("备注12");
            let al1: &str = data.get("备注13");
            let am1: &str = data.get("备注14");
            let an1: &str = data.get("备注15");
            let ao1: &str = data.get("备注16");
            let ap1: &str = data.get("备注17");
            let aq1: &str = data.get("备注18");
            let ar1: &str = data.get("备注19");
            let as1: &str = data.get("备注20");
            let at1: &str = data.get("备注21");
            let au1: &str = data.get("备注22");
            let av1: &str = data.get("备注23");
            let aw1: &str = data.get("备注24");
            let ax1: &str = data.get("备注25");
            let ay1: &str = data.get("备注26");
            let az1: &str = data.get("备注27");
            let ba1: &str = data.get("备注28");
            let bb1: &str = data.get("备注29");
            let bc1: &str = data.get("备注30");
            csv_writer_gl.write_record(&[
                a1.to_string(), b1.to_string(), c1.to_string(), d1.to_string(), e1.to_string(), f1.to_string(),
                g1.to_string(), h1.to_string(), i1.to_string(), j1.to_string(), k1.to_string(), l1.to_string(),
                m1.to_string(), n1.to_string(), o1.to_string(), p1.to_string(), q1.to_string(), r1.to_string(),
                s1.to_string(), t1.to_string(), u1.to_string(), v1.to_string(), w1.to_string(), x1.to_string(),
                y1.to_string(), z1.to_string(), aa1.to_string(), ab1.to_string(), ac1.to_string(), ad1.to_string(),
                ae1.to_string(), af1.to_string(), ag1.to_string(), ah1.to_string(), ai1.to_string(), aj1.to_string(),
                ak1.to_string(), al1.to_string(), am1.to_string(), an1.to_string(), ao1.to_string(), ap1.to_string(),
                aq1.to_string(), ar1.to_string(), as1.to_string(), at1.to_string(), au1.to_string(), av1.to_string(),
                aw1.to_string(), ax1.to_string(), ay1.to_string(), az1.to_string(), ba1.to_string(), bb1.to_string(),
                bc1.to_string()]).unwrap();
        }
        csv_writer_gl.flush()?;
        start += step_i32;
        file_count += 1;
    }

    // query tb
    let sql_query_tb = format!(
            "SELECT 被审计单位, 审计期间, 科目编号, 科目名称, CAST(科目级次 AS CHAR) AS 科目级次, 科目类别,币种, 借贷方向, 
                CAST(`期初数-外币` AS CHAR) AS `期初数-外币`, 
                CAST(期初数 AS CHAR) AS 期初数, CAST(`借方发生额-外币` AS CHAR) AS `借方发生额-外币`, CAST(借方发生额 AS CHAR) AS 借方发生额,
                CAST(`贷方发生额-外币` AS CHAR) AS `贷方发生额-外币`, CAST(贷方发生额 AS CHAR) AS 贷方发生额, CAST(期末数外币 AS CHAR) AS 期末数外币,
                CAST(期末数 AS CHAR) AS 期末数, CAST(期初数量 AS CHAR) AS 期初数量, CAST(借方数量 AS CHAR) AS 借方数量,
                CAST(贷方数量 AS CHAR) AS 贷方数量, CAST(期末数量 AS CHAR) AS 期末数量
            FROM {}.科目余额表", get_code_str);
    let data_tb = query(&sql_query_tb).fetch_all(&pool).await.unwrap();
    let output_path = format!("{}/{}_TB.csv", save_path, filename);
    let mut csv_writer_tb = WriterBuilder::new()
        .delimiter(b'|')
        .from_path(output_path)
        .unwrap();
    csv_writer_tb.write_record(&[
        "被审计单位", "审计期间", "科目编号", "科目名称", "科目级次", "科目类别", "币种", "借贷方向", "期初数-外币", "期初数",
        "借方发生额-外币", "借方发生额", "贷方发生额-外币", "贷方发生额", "期末数外币", "期末数", "期初数量",
        "借方数量", "贷方数量", "期末数量"
        ]).unwrap();
    for data in data_tb {
        let t1: &str = data.get("被审计单位");
        let t2: &str = data.get("审计期间");
        let t3: &str = data.get("科目编号");
        let t4: &str = data.get("科目名称");
        let t5: &str = data.get("科目级次");
        let t6: &str = data.get("科目类别");
        let t7: &str = data.get("币种");
        let t8: &str = data.get("借贷方向");
        let t9: &str = data.get("期初数-外币");
        let t10: &str = data.get("期初数");
        let t11: &str = data.get("借方发生额-外币");
        let t12: &str = data.get("借方发生额");
        let t13: &str = data.get("贷方发生额-外币");
        let t14: &str = data.get("贷方发生额");
        let t15: &str = data.get("期末数外币");
        let t16: &str = data.get("期末数");
        let t17: &str = data.get("期初数量");
        let t18: &str = data.get("借方数量");
        let t19: &str = data.get("贷方数量");
        let t20: &str = data.get("期末数量");
        csv_writer_tb.write_record(&[
            t1.to_string(), t2.to_string(), t3.to_string(), t4.to_string(), t5.to_string(), t6.to_string(),
            t7.to_string(), t8.to_string(), t9.to_string(), t10.to_string(), t11.to_string(), t12.to_string(),
            t13.to_string(), t14.to_string(), t15.to_string(), t16.to_string(), t17.to_string(), t18.to_string(),
            t19.to_string(), t20.to_string()]).unwrap();
    }
    csv_writer_tb.flush()?;
    Ok(())
}

#[pyfunction]
pub fn conn_sql(url: &str, url_sql: &str, company_name: &str, save_path: &str) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(conn_mysql(url, url_sql, company_name, save_path)).unwrap();
    Ok(())
}

#[pymodule]
fn pycrab(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(filter_row, m)?)?;
    m.add_function(wrap_pyfunction!(filter_rows, m)?)?;
    m.add_function(wrap_pyfunction!(merge_csv, m)?)?;
    m.add_function(wrap_pyfunction!(conn_sql, m)?)?;
    Ok(())
}