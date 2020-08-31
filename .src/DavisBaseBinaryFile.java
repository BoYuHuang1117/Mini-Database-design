import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.SortedMap;

import java.util.ArrayList;
import java.lang.Math.*;
import java.util.Arrays;
import static java.lang.System.out;
import java.util.List;
import java.util.Map;

/**
 *
 * Author: Bo Yu Huang
 * Date: 7/21/20
 */
public class DavisBaseBinaryFile {

    static String columnsTable = "davisbase_columns";
    static String tablesTable = "davisbase_tables";
    static boolean showRowId = false;
    static boolean dataStoreInitialized = false;

    /*
     * Page size for all files is 512 bytes by default.
     * You may choose to make it user modifiable
     *
     * This static variable controls page size.
     * This strategy insures that the page size is always a power of 2.
     */
    static int pageSizePower = 9;
    static int pageSize = (int) Math.pow(2, pageSizePower);

    RandomAccessFile _file;

    DavisBaseBinaryFile(RandomAccessFile file) {
        _file = file;
    }

    // Find the root page
    public static int getRootPageNo(RandomAccessFile tblFile) {
        try {
            for (int i = 0; i < tblFile.length() / pageSize; i++) {
                tblFile.seek(i * pageSize + 0x0A);
                if (tblFile.readInt() == -1) { // 0xFFFFFFFF
                    return i;
                }
            }
            return 0;
        } catch (Exception e) {
            out.println("ERROR: unable to get root page number " + e);
        }
        return -1;
    }

    // Find the last ID in certain page
    public static int getPageLastID(RandomAccessFile tblFile, int pageNum){
        try{
            // look for the offset of start of the cell in header and search for row_id in cell header
            tblFile.seek(pageNum * pageSize + 4);
            short startOfCell = tblFile.readShort();
            if (startOfCell == pageSize) // empty page
                return 0;
            tblFile.seek(pageNum * pageSize + startOfCell + 2);
            return tblFile.readInt();
        }catch (Exception e){
            out.println("ERROR: unable to get page last_ID! " + e);
        }
        return -1;
    }


    public boolean recordExists(TableInfo tableInfo, WhereCondition condition) throws IOException {
        BPlusTree bPlusTree = new BPlusTree(tableInfo._rootPageNum, tableInfo._tableName, _file);

        for(Integer pageNo :  bPlusTree._leavesNum) {
            Page page = new Page(pageNo,_file);
            for(TableRow record : page.getPageRows()) {
                if(condition!=null) {
                    if(!condition.checkCondition(record._fields.get(condition._columnOrdinal)._strValue))
                        continue;
                }
                return true;
            }
        }
        return false;
    }

    public int updateRecords(TableInfo tableInfo, WhereCondition condition_1, WhereCondition condition_2, int whereConnect, List<String> colNames, List<String> newValues) throws IOException {
        int count = 0;

        List<Integer> ordinalPostions = tableInfo.getOrdinalPostions(colNames);

        //map new values to column ordinal position
        int k=0;
        Map<Integer,Field> newValueMap = new HashMap<>();

        for(String strnewValue:newValues){
            int index = ordinalPostions.get(k);
            try{
                newValueMap.put(index, new Field(tableInfo._colData.get(index)._type,strnewValue));
            }
            catch (Exception e) {
                System.out.println("ERROR: Invalid data format for " + tableInfo._colNames.get(index) + " values: " + strnewValue);
                return count;
            }
            k++;
        }

        BPlusTree bPlusTree = new BPlusTree(tableInfo._rootPageNum,tableInfo._tableName, _file);

        List<Integer> updateRowids = new ArrayList<>();
        for(Integer pageNo :  bPlusTree._leavesNum) {
            short deleteCountPerPage = 0;
            Page page = new Page(pageNo, _file);
            for (TableRow record : page.getPageRows()) {
                if (whereConnect == 0 || condition_2 == null) {
                    if (condition_1 != null) {
                        if (!condition_1.checkCondition(record._fields.get(condition_1._columnOrdinal)._strValue))
                            continue;
                    }
                }
                else if (whereConnect == 1){
                    // AND situation
                    if (condition_1 != null) {
                        if (!condition_1.checkCondition(record._fields.get(condition_1._columnOrdinal)._strValue))
                            continue;
                    }
                    if (condition_2!=null){
                        if(!condition_2.checkCondition(record._fields.get(condition_2._columnOrdinal)._strValue))
                            continue;
                    }
                }
                else if (whereConnect == 2){
                    // OR situation
                    if (condition_1 != null && condition_2!=null) {
                        if (!condition_1.checkCondition(record._fields.get(condition_1._columnOrdinal)._strValue)
                            && !condition_2.checkCondition(record._fields.get(condition_2._columnOrdinal)._strValue))
                            continue;
                    }
                }
                if(!updateRowids.contains(record._rowId))
                    count++;

                List<Field> attrs = record._fields;
                for(int i :newValueMap.keySet()) {
                    int rowId = record._rowId;
                    if((record._fields.get(i)._type == Type.TEXT && record._fields.get(i)._strValue.length() == newValueMap.get(i)._strValue.length())
                            || (record._fields.get(i)._type != Type.NULL && record._fields.get(i)._type != Type.TEXT)){
                        page.updateRecord(record, i, newValueMap.get(i)._ByteAtt);
                        Field attr = attrs.get(i);
                        attrs.remove(i);
                        attr = newValueMap.get(i);
                        attrs.add(i, attr);
                    }
                    else{
                        page.DeleteTBLRecord(tableInfo._tableName, Integer.valueOf(record._pageHeaderIndex - deleteCountPerPage).shortValue());
                        deleteCountPerPage++;

                        Field attr = attrs.get(i);
                        attrs.remove(i);
                        attr = newValueMap.get(i);
                        attrs.add(i, attr);
                        int pageNoToinsert = BPlusTree.getPageNoForInsert(_file,tableInfo._rootPageNum);
                        Page pageToInsert = new Page(pageNoToinsert, _file);
                        rowId =  pageToInsert.addTableRow(tableInfo._tableName , attrs);
                        updateRowids.add(rowId);
                    }
                }
            }
        }
        if(!tableInfo._tableName.equals(tablesTable) && !tableInfo._tableName.equals(columnsTable))
            System.out.println(count+" record(s) updated.");

        return count;
    }

    public void selectRecords(TableInfo tableInfo, List<String> columnNames, WhereCondition condition_1, WhereCondition condition_2, int whereConnect) throws IOException{
        //The select order might be different from the table ordinal position
        List<Integer> ordinalPostions = tableInfo.getOrdinalPostions(columnNames);

        System.out.println();

        List<Integer> printPosition = new ArrayList<>();

        int columnPrintLength = 0;
        printPosition.add(columnPrintLength);
        int totalTablePrintLength =0;
        if(showRowId){
            System.out.print("rowid");
            System.out.print(DavisBasePrompt.line(" ",5));
            printPosition.add(10);
            totalTablePrintLength +=10;
        }

        for(int i:ordinalPostions){
            String columnName = tableInfo._colData.get(i)._columnName;
            columnPrintLength = Math.max(columnName.length(),tableInfo._colData.get(i)._type.getPrintOffset()) + 5;
            printPosition.add(columnPrintLength);
            System.out.print(columnName);
            System.out.print(DavisBasePrompt.line(" ",columnPrintLength - columnName.length() ));
            totalTablePrintLength +=columnPrintLength;
        }
        System.out.println();
        System.out.println(DavisBasePrompt.line("-",totalTablePrintLength));

        BPlusTree bPlusTree = new BPlusTree(tableInfo._rootPageNum,tableInfo._tableName, _file);

        String currentValue = "";
        int count = 0;
        for(Integer pageNo : bPlusTree._leavesNum) {
            Page page = new Page(pageNo, _file);
            for(TableRow record : page.getPageRows()) {
                if (whereConnect == 0 || condition_2 == null) {
                    if (condition_1 != null) {
                        if (!condition_1.checkCondition(record._fields.get(condition_1._columnOrdinal)._strValue))
                            continue;
                    }
                }
                else if (whereConnect == 1){
                    // AND situation
                    if (condition_1 != null) {
                        if (!condition_1.checkCondition(record._fields.get(condition_1._columnOrdinal)._strValue))
                            continue;
                    }
                    if (condition_2 != null){
                        if(!condition_2.checkCondition(record._fields.get(condition_2._columnOrdinal)._strValue))
                            continue;
                    }
                }
                else if (whereConnect == 2){
                    // OR situation
                    if (condition_1 != null && condition_2 != null) {
                        if (!condition_1.checkCondition(record._fields.get(condition_1._columnOrdinal)._strValue)
                                && !condition_2.checkCondition(record._fields.get(condition_2._columnOrdinal)._strValue))
                            continue;
                    }
                }
                int columnCount = 0;
                if(showRowId){
                    currentValue = Integer.valueOf(record._rowId).toString();
                    System.out.print(currentValue);
                    System.out.print(DavisBasePrompt.line(" ",printPosition.get(++columnCount) - currentValue.length()));
                }
                for(int i :ordinalPostions) {
                    currentValue = record._fields.get(i)._strValue;
                    System.out.print(currentValue);
                    System.out.print(DavisBasePrompt.line(" ",printPosition.get(++columnCount) - currentValue.length()));
                }
                System.out.println();
                count++;
            }
        }
        // reset the showRowId to default value
        showRowId = false;
        System.out.println();
        System.out.println(count + " records retrieved.");
    }

    /**
     * This static method creates the DavisBase data storage container and then
     * initializes two .tbl files to implement the two system tables,
     * davisbase_tables and davisbase_columns
     *
     * WARNING! Calling this method will destroy the system database catalog files
     * if they already exist.
     */
    public static void initializeDataStore() {

        /* Create data directory at the current OS location to hold */
        try {
            File dataDir = new File("data");
            dataDir.mkdir();
            dataDir = new File ("data/catalog");
            dataDir.mkdir();
            String[] oldTableFiles;
            oldTableFiles = dataDir.list();
            for (int i = 0; i < oldTableFiles.length; i++) {
                File anOldFile = new File(dataDir, oldTableFiles[i]);
                anOldFile.delete();
            }
        } catch (SecurityException se) {
            out.println("Unable to create data container directory");
            out.println(se.getMessage());
        }

        /* Create davisbase_tables system catalog */
        try {
            int currentPageNo = 0;

            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(tablesTable), "rw");
            Page.addNewPage(davisbaseTablesCatalog, PageType.tblLEAF, -1, -1);
            Page page = new Page(currentPageNo, davisbaseTablesCatalog);

            page.addTableRow(tablesTable,Arrays.asList(new Field(Type.TEXT, DavisBaseBinaryFile.tablesTable),
                    new Field(Type.SMALLINT, "0"),
                    new Field(Type.INT, "2"),
                    new Field(Type.INT, "2")));

            page.addTableRow(tablesTable,Arrays.asList(new Field(Type.TEXT, DavisBaseBinaryFile.columnsTable),
                    new Field(Type.SMALLINT, "2"),
                    new Field(Type.INT, "11"),
                    new Field(Type.INT, "11")));
            davisbaseTablesCatalog.close();
        } catch (Exception e) {
            out.println("Unable to create the database_tables file");
            out.println(e.getMessage());
        }

        /* Create davisbase_columns systems catalog */
        try {
            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(columnsTable), "rw");
            Page.addNewPage(davisbaseColumnsCatalog, PageType.tblLEAF, -1, -1);
            Page page = new Page(0, davisbaseColumnsCatalog);

            short ordinal_position = 1;

            //Add new columns to davisbase_tables
            page.addNewColumn(new TableCol(tablesTable,Type.TEXT, "table_name", true, false, ordinal_position++));
            page.addNewColumn(new TableCol(tablesTable,Type.SMALLINT, "root_page", false, false, ordinal_position++));
            page.addNewColumn(new TableCol(tablesTable,Type.INT, "last_id", false, false, ordinal_position++));
            page.addNewColumn(new TableCol(tablesTable,Type.INT, "record_count", false, false, ordinal_position));

            //Add new columns to davisbase_columns
            ordinal_position = 1;

            page.addNewColumn(new TableCol(columnsTable,Type.TEXT, "table_name", false, false, ordinal_position++));
            page.addNewColumn(new TableCol(columnsTable,Type.TEXT, "column_name", false, false, ordinal_position++));
            page.addNewColumn(new TableCol(columnsTable,Type.SMALLINT, "data_type", false, false, ordinal_position++));
            page.addNewColumn(new TableCol(columnsTable,Type.SMALLINT, "ordinal_position", false, false, ordinal_position++));
            page.addNewColumn(new TableCol(columnsTable,Type.TEXT, "is_nullable", false, false, ordinal_position++));
            page.addNewColumn(new TableCol(columnsTable,Type.SMALLINT, "primary_key", false, true, ordinal_position++));
            page.addNewColumn(new TableCol(columnsTable,Type.SMALLINT, "is_unique", false, false, ordinal_position));

            davisbaseColumnsCatalog.close();
            dataStoreInitialized = true;
        } catch (Exception e) {
            out.println("Unable to create the database_columns file");
            out.println(e.getMessage());
        }
    }
}


