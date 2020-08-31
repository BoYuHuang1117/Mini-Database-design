# Mini-Database-design

### The mini version of the database support _create, insert, update, delete, drop tables and where clause_.

All of them are SQL-like commands and data is stored in a B+ tree style. All the file is contained using .tbl file with reading and writing using RandomAccessFile class. 

This mini database automatically create a "data" repository and a "catalog" directory within it along with two meta-data named "tablesTable" and "columnsTable".

### The page size of each .tbl file is 512 KB. It support datatype such as _Byte, Short, Integer, Float, Double and String_ etc.

The detail Storage Definition Language (SDL) is in the "DavisBase Nano File Format Guide (SDL).pdf".
