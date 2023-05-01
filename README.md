# pycrab - v0.1.0

#### usage

```python
import pycrab

# single filter row
pycrab.filter_row(
    csv_path="filter.csv",  # csv path
    output_path=r"save/save.csv",  # output path
    sep=list(b',')[0],  # delimiter
    col=0,  # column index
    cond="apple",  # condtion
    is_exac=True  # exact filter
)

# connect mysql
pycrab.conn_sql(
    url="mysql://root:password@localhost/database",
    url_sql="mysql://root:password@localhost/database",
    company_name="project_name",
    save_path=r"E:\Desktop\test_data\test"
)
```

#### function

- filter_row
- filter_rows
- merge_csv
- conn_mysql
