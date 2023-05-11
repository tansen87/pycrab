# pycrab - v0.1.1

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

#### function

- filter_row
- filter_rows
- merge_csv
- split_csv
