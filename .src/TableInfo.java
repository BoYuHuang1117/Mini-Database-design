import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

/**
 * Author: Bo-Yu Huang
 * Date: 7/25/20
 */

class TableRow{
    public int _rowId;
    public Byte[] _colDatatypes;
    public Byte[] _recordBody;
    public List<Field> _fields;
    public short _recordOffset;
    public short _pageHeaderIndex;
    
    TableRow(short pageHeaderIndex, short recordOffset, byte[] colDatatypes, byte[] recordBody, int rowId) throws Exception{
        _pageHeaderIndex = pageHeaderIndex;
        _recordOffset = recordOffset;
        _colDatatypes = LoadByte.byteToBytes(colDatatypes);
        _recordBody = LoadByte.byteToBytes(recordBody);
        _rowId = rowId;
        setFields();
    }
    
    private void setFields() throws Exception {
        _fields = new ArrayList<>();
        int pointer = 0;
        for(Byte colDataType : _colDatatypes) {
             byte[] byteValue = LoadByte.Bytestobytes(Arrays.copyOfRange(_recordBody,pointer, pointer + Type.getTypeSize(colDataType)));
             _fields.add(new Field(Type.get(colDataType), byteValue));
                    pointer =  pointer + Type.getTypeSize(colDataType);
        }
    }
}

public class TableInfo{
    public boolean _tableExist = false;
    public String _tableName;
    public List<TableRow> _rowData;
    public List<TableCol> _colData;
    public List<String> _colNames;

    public int _rootPageNum;
    public int _last_id;
    public int _rowCount;

    TableInfo(String tableName){
        _tableName = tableName;
        try{
            RandomAccessFile TablesCatalog = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(DavisBaseBinaryFile.tablesTable), "r");
            BPlusTree bplusTree = new BPlusTree(DavisBaseBinaryFile.getRootPageNo(TablesCatalog), tableName, TablesCatalog);
            //scan through all pages in tables-catalog
            for (Integer pageNo : bplusTree.getAllLeaves()) {
                Page page = new Page(pageNo, TablesCatalog);
                //scan through all the records in each page
                for (TableRow record : page.getPageRows()) {
                    //if the record with table is found, get the root page No and record count; break the loop
                    if ((record._fields.get(0)._strValue).equals(tableName)) {
                        _rootPageNum = Integer.parseInt(record._fields.get(1)._strValue);
                        _last_id = Integer.parseInt(record._fields.get(2)._strValue);
                        _rowCount = Integer.parseInt(record._fields.get(3)._strValue);
                        _tableExist = true;
                        break;
                    }
                }
                if(_tableExist)
                    break;
            }

            TablesCatalog.close();
            if(_tableExist){
                // load column data
                try {
                    RandomAccessFile columnsCatalog = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(DavisBaseBinaryFile.columnsTable), "r");
                    _rowData = new ArrayList<>();
                    _colData = new ArrayList<>();
                    _colNames = new ArrayList<>();
                    BPlusTree bPlusTree = new BPlusTree(DavisBaseBinaryFile.getRootPageNo(columnsCatalog), _tableName,columnsCatalog);
         
                    /* Get all columns from the davisbase_columns, loop through all the leaf pages
                    and find the records with the table name */
                    for (Integer pageNo : bPlusTree.getAllLeaves()) {
                        Page page = new Page(pageNo, columnsCatalog);
                        for (TableRow record : page.getPageRows()) {
                            if (record._fields.get(0)._strValue.equals(_tableName)) {
                                //set column information in the data members of the class
                                _rowData.add(record);
                                _colNames.add(record._fields.get(1)._strValue);
                                TableCol colInfo = new TableCol(
                                        _tableName, Type.get(record._fields.get(2)._strValue),
                                        record._fields.get(1)._strValue, record._fields.get(6)._strValue.equals("YES"),
                                        record._fields.get(4)._strValue.equals("YES"), Short.parseShort(record._fields.get(3)._strValue)
                                );

                                if(record._fields.get(5)._strValue.equals("PRI"))
                                    colInfo._isPrimaryKey = true;

                                _colData.add(colInfo);
                            }
                        }
                    }
                    // 'rowid' column doesn't exist here
                    columnsCatalog.close();
                } catch (Exception e) {
                    System.out.println("ERROR: unable to get complete column data for " + _tableName);
                }
            }
            else
                throw new Exception("Table does not exist.");
        } catch (Exception e){
            System.out.println("ERROR: Unable to check if table " + _tableName + " exists!" + e);
        }
    }

    public boolean checkColumnExists(List<String> columns) {
        if(columns.size() == 0)
            return true;
        // if rowid exist in query, include it automatically
        if (columns.contains("rowid")){
            DavisBaseBinaryFile.showRowId = true;
            columns.remove("rowid");
        }
        List<String> lColumns = new ArrayList<>(columns);

        for (TableCol col : _colData) {
            if (lColumns.contains(col._columnName))
                lColumns.remove(col._columnName);
        }
        return lColumns.isEmpty();
    }
    
    public List<Integer> getOrdinalPostions(List<String> columns){
        List<Integer> ordinalPostions = new ArrayList<>();
        for(String column : columns)
            ordinalPostions.add(_colNames.indexOf(column));
        return ordinalPostions;
    }

    public void updateCatalog(){
        // update root page, last_id and record_count in the tables catalog
        try{
            RandomAccessFile tableFile = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(_tableName), "r");
            // re-found the root page number
            int rootPageNo = DavisBaseBinaryFile.getRootPageNo(tableFile);
            tableFile.close();

            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(DavisBaseBinaryFile.tablesTable), "rw");
            DavisBaseBinaryFile tablesBinaryFile = new DavisBaseBinaryFile(davisbaseTablesCatalog);

            TableInfo tablesInfo = new TableInfo(DavisBaseBinaryFile.tablesTable);

            // set condition to find the corresponding row in the catalog
            WhereCondition condition = new WhereCondition(Type.TEXT);
            condition._columnName = "table_name";
            condition._columnOrdinal = 0;
            condition.setConditionValue(_tableName);
            condition.setOperator("=");

            // list the columns and values to be updated in order
            List<String> columns = Arrays.asList("root_page", "last_id", "record_count");
            List<String> newValues = new ArrayList<>();

            newValues.add(new Integer(rootPageNo).toString());
            newValues.add(new Integer(_last_id).toString());
            newValues.add(new Integer(_rowCount).toString());
            // list the columns and values to be updated in order

            tablesBinaryFile.updateRecords(tablesInfo,condition,null,0,columns,newValues);

            davisbaseTablesCatalog.close();
        } catch(IOException e){
            System.out.println("Error: unable to update meta data for " + _tableName);
        }
    }

    public boolean validateInsert(List<Field> row) throws IOException{
        RandomAccessFile tableFile = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(_tableName), "r");
        DavisBaseBinaryFile file = new DavisBaseBinaryFile(tableFile);

        for(int i=0;i<_colData.size();i++) {
            WhereCondition condition = new WhereCondition(_colData.get(i)._type);
            condition._columnName = _colData.get(i)._columnName;
            condition._columnOrdinal = i;
            condition.setOperator("=");

            if(_colData.get(i)._isUnique)
            {
                condition.setConditionValue(row.get(i)._strValue);
                if(file.recordExists(this, condition)){
                    System.out.println("ERROR: Insert failed: Column "+ _colData.get(i)._columnName + " should be unique.");
                    tableFile.close();
                    return false;
                }
            }
        }
        tableFile.close();
        return true;
    }
}

class TableCol{
    public Type _type;
    
    public String _columnName;

    public boolean _isUnique;
    public boolean _isNullable;
    public Short _ordinalPosition;
    public String _tableName;
    public boolean _isPrimaryKey;

    TableCol(){ }
    TableCol(String tableName,Type type,String columnName,boolean isUnique,boolean isNullable,short ordinalPosition){
        _type = type;
        _columnName = columnName;
        _isUnique = isUnique;
        _isNullable = isNullable;
        _ordinalPosition = ordinalPosition;
        _tableName = tableName;
    }
}