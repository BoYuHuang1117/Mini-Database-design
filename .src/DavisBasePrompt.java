import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static java.lang.System.out;

/**
 * Author: Bo-Yu Huang
 * Date: 7/21/20
 */
public class DavisBasePrompt {

    static String prompt = "BY_Query> ";
    static String version = "v0.9";
    static String copyright = "Bo-Yu_Huang";

    static boolean isExit = false;
    /*
     * The Scanner class is used to collect user commands from the prompt There are
     * many ways to do this. This is just one.
     *
     * Each time the semicolon (;) delimiter is entered, the userCommand String is
     * re-populated.
     */
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    /**
     * ******** Main method ******************
     */
    public static void main(String[] args) {
        /* Display the welcome screen */
        splashScreen();

        File dataDir = new File("data/catalog/");

        if (!new File(dataDir, DavisBaseBinaryFile.tablesTable + ".tbl").exists()
                || !new File(dataDir, DavisBaseBinaryFile.columnsTable + ".tbl").exists())
            DavisBaseBinaryFile.initializeDataStore();
        else
            DavisBaseBinaryFile.dataStoreInitialized = true;

        /* Variable to collect user input from the prompt */
        String userCommand = "";

        while (!isExit) {
            System.out.print(prompt);
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
        System.out.println("Exiting from BY_Query");
    }

    /**
     * ***********************************************************************
     * Static method definitions
     */

    /**
     * Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(line("-", 60));
        System.out.println("This is BY_Query"); // Display the string.
        System.out.println("BY_Query Version " + getVersion());
        System.out.println(getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(line("-", 80));
    }

    public static String line(String ch, int num) {
        String a = "";
        for (int i = 0; i < num; i++)
            a += ch;
        return a;
    }

    /**
     * Help: Display supported commands
     */
    public static void help() {
        out.println(line("*", 100));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");

        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");

        out.println("CREATE TABLE <table_name> (<column_name> <data_type> <primary key> <not null>, ...);");
        out.println("\tCreates a table with the given columns.\n");

        out.println("DROP TABLE <table_name>;");
        out.println("\tRemoves table data (i.e. all records) and its schema as well as any indexes.\n");

        out.println("UPDATE TABLE <table_name> SET <column_name> = <value> WHERE <condition>;");
        out.println("\tModify records data whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");
        out.println("\tInserts a new record into the table with the given values for the given columns.\n");

        out.println("DELETE FROM TABLE <table_name> WHERE <condition>;");
        out.println("\tDelete table records whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("SELECT <column_list> FROM <table_name> WHERE <condition>;");
        out.println("\tDisplay table records whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("SOURCE <filename>;");
        out.println("\tProcess a batch file of commands.\n");

        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");

        out.println("HELP;");
        out.println("\tDisplay this help information.\n");

        out.println("EXIT;");
        out.println("\tExit the program.\n");

        out.println(line("*", 80));
    }

    /** return the DavisBase version */
    public static String getVersion() {
        return version;
    }

    public static String getCopyright() {
        return copyright;
    }

    public static void displayVersion() {
        System.out.println("SQVeryLite Version " + getVersion());
        System.out.println(getCopyright());
    }
    
    public static String getTBLFilePath(String tableName) {
        if (tableName.equals(DavisBaseBinaryFile.columnsTable) || tableName.equals(DavisBaseBinaryFile.tablesTable))
            return "data/catalog/" + tableName + ".tbl";
        File dataDir = new File ("data/user_data");
        if (!dataDir.isDirectory())
            dataDir.mkdir();
        return "data/user_data/" + tableName + ".tbl";
    }
    
    public static void parseUserCommand(String userCommand) {

        userCommand = userCommand.replaceAll("\n", " ");    // Remove newlines
        userCommand = userCommand.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space

        /*
         * commandTokens is an array of Strings that contains one token per array
         * element The first token can be used to determine the type of command The
         * other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement
         */
        ArrayList<String> commandTokens = new ArrayList<>(Arrays.asList(userCommand.split(" ")));

        /*
         * This switch handles a very small list of hardcoded commands of known syntax.
         * You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0)) {
            case "show":
                if (commandTokens.get(1).equals("tables"))
                    parseUserCommand("select * from davisbase_tables");
                else
                    System.out.println("ERROR: I didn't understand the command: \"" + userCommand + "\"");
                break;
            case "select":
                parseQuery(userCommand);
                break;
            case "create":
                if (commandTokens.get(1).equals("table"))
                    parseCreateTable(userCommand);
                else
                    System.out.println("I only support 'create table' command! I didn't understand the command: \"" + userCommand + "\"");
                break;
            case "insert":
                parseInsert(userCommand);
                break;
            case "delete":
                parseDelete(userCommand);
                break;
            case "update":
                parseUpdate(userCommand);
                break;
            case "drop":
                dropTable(userCommand);
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
            case "quit":
                isExit = true;
                break;
            default:
                System.out.println("ERROR: I don't understand the command: \"" + userCommand + "\"");
                break;
        }
    }

    /**
     * Stub method for executing queries
     *
     * @param queryString is a String of the user input
     */
    public static void parseQuery(String queryString) {
        String table_name = "";
        List<String> column_names = new ArrayList<>();

        // Get table and column names for the select
        ArrayList<String> queryTableTokens = new ArrayList<>(Arrays.asList(queryString.split(" ")));

        for (int i = 1; i < queryTableTokens.size(); i++) {
            if (queryTableTokens.get(i).equals("from")) {
                ++i;
                table_name = queryTableTokens.get(i);
                break;
            }
            if (!queryTableTokens.get(i).equals("*") && !queryTableTokens.get(i).equals(",")) {
                if (queryTableTokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<>(Arrays.asList(queryTableTokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(queryTableTokens.get(i));
            }
        }

        TableInfo tableInfo = new TableInfo(table_name);
        if(!tableInfo._tableExist) {
            System.out.println("ERROR: Table does not exist");
            return;
        }

        if (!tableInfo.checkColumnExists(column_names)) {
            System.out.println("ERROR: Invalid column name(s)");
            return;
        }

        /// get AND(1), OR(2)
        int whereConnect = 0;
        String condstring_1 = queryString;
        String condstring_2 = "where ";
        if (queryTableTokens.contains("and")) {
            condstring_1 = queryString.substring(0,queryString.indexOf("and"));
            condstring_2 += queryString.substring(queryString.indexOf("and")+4,queryString.length());
            //out.println("cond1: " + condstring_1);
            //out.println("cond2: " + condstring_2);
            whereConnect = 1;
        }
        else if (queryTableTokens.contains("or")){
            condstring_1 = queryString.substring(0,queryString.indexOf("or"));
            condstring_2 += queryString.substring(queryString.indexOf("or")+3,queryString.length());
            whereConnect = 2;
        }

        WhereCondition condition_1 = null;
        try {
            condition_1 = WhereCondition.extractConditionFromQuery(tableInfo, condstring_1);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        WhereCondition condition_2 = null;
        if (whereConnect != 0) {
            try{
                condition_2 = WhereCondition.extractConditionFromQuery(tableInfo, condstring_2);
            }catch (Exception e){
                System.out.println(e.getMessage());
                return;
            }
        }

        if (column_names.size() == 0)
            column_names = tableInfo._colNames;

        try {
            RandomAccessFile tableFile = new RandomAccessFile(getTBLFilePath(table_name), "r");
            DavisBaseBinaryFile tableBinaryFile = new DavisBaseBinaryFile(tableFile);
            tableBinaryFile.selectRecords(tableInfo, column_names, condition_1, condition_2, whereConnect);
            tableFile.close();
        } catch (IOException exception) {
            System.out.println("Error selecting columns from table");
        }
    }

    /**
     * Stub method for dropping tables
     * @param dropTableString is a String of the user input
     */
    public static void dropTable(String dropTableString) {
        /**
         * delete row related to the dropped table in two catalogs
         */
        String[] tokens = dropTableString.split(" ");
        if(!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println("Syntax Error");
            System.out.println("Expected Syntax: DROP TABLE [table_name]; ");
            return;
        }

        ArrayList<String> dropTableTokens = new ArrayList<>(Arrays.asList(dropTableString.split(" ")));
        String tableName = dropTableTokens.get(2);

        parseDelete("delete from "+ DavisBaseBinaryFile.tablesTable + " where table_name = '"+tableName+"' ");
        parseDelete("delete from "+ DavisBaseBinaryFile.columnsTable + " where table_name = '"+tableName+"' ");
        File tableFile = new File("data/user_data/"+tableName+".tbl");
        if(tableFile.delete()){
            System.out.println("Dropped " + tableName);
        }else
            System.out.println("ERROR: Table not exist");
    }

    /**
     * Stub method for updating records
     *
     * @param updateString is a String of the user input
     */
    public static void parseUpdate(String updateString) {
        ArrayList<String> updateTokens = new ArrayList<>(Arrays.asList(updateString.split(" ")));

        String table_name = updateTokens.get(1);
        List<String> columnsToUpdate = new ArrayList<>();
        List<String> valueToUpdate = new ArrayList<>();

        if (!updateTokens.get(2).equals("set") || !updateTokens.contains("=")) {
            System.out.println("Syntax error");
            System.out.println("Expected Syntax: UPDATE [table_name] SET [Column_name] = value1 where [column_name] = value2; ");
            return;
        }

        String updateColInfoString = updateString.split("set")[1].split("where")[0];

        List<String> column_newValueSet = Arrays.asList(updateColInfoString.split(","));

        try {
            for (String item : column_newValueSet) {
                columnsToUpdate.add(item.split("=")[0].trim());
                valueToUpdate.add(item.split("=")[1].trim().replace("\"", "").replace("'", ""));
            }
        } catch (Exception e) {
            System.out.println("Syntax error");
            System.out.println(
                    "Expected Syntax: UPDATE [table_name] SET [Column_name] = val1 where [column_name] = val2; ");
            return;
        }

        TableInfo tableInfo = new TableInfo(table_name);

        if (!tableInfo._tableExist) {
            System.out.println("ERROR: Table name not exist");
            return;
        }

        if (!tableInfo.checkColumnExists(columnsToUpdate)) {
            System.out.println("ERROR: Invalid column name(s)");
            return;
        }

        // get AND(1), OR(2)
        int whereConnect = 0;
        String condstring_1 = updateString;
        String condstring_2 = "where ";
        if (updateTokens.contains("and")) {
            condstring_1 = updateString.substring(0,updateString.indexOf("and"));
            condstring_2 += updateString.substring(updateString.indexOf("and")+4,updateString.length());
            //out.println("cond1: " + condstring_1);
            //out.println("cond2: " + condstring_2);
            whereConnect = 1;
        }
        else if (updateTokens.contains("or")){
            condstring_1 = updateString.substring(0,updateString.indexOf("or"));
            condstring_2 += updateString.substring(updateString.indexOf("or")+3,updateString.length());
            whereConnect =2;
        }

        WhereCondition condition_1 = null;
        try {
            condition_1 = WhereCondition.extractConditionFromQuery(tableInfo, condstring_1);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        WhereCondition condition_2 = null;
        if (whereConnect != 0) {
            try{
                condition_2 = WhereCondition.extractConditionFromQuery(tableInfo, condstring_2);
            }catch (Exception e){
                System.out.println(e.getMessage());
                return;
            }
        }
        try {
            RandomAccessFile file = new RandomAccessFile(getTBLFilePath(table_name), "rw");
            DavisBaseBinaryFile binaryFile = new DavisBaseBinaryFile(file);
            binaryFile.updateRecords(tableInfo, condition_1, condition_2, whereConnect, columnsToUpdate, valueToUpdate);

            file.close();
        } catch (Exception e) {
            out.println("Unable to update the " + table_name + " file");
            out.println(e);
        }
    }

    public static void parseInsert(String queryString) {
        ArrayList<String> insertTokens = new ArrayList<>(Arrays.asList(queryString.split(" ")));

        if (!insertTokens.get(1).equals("into") || !queryString.contains(") values")) {
            System.out.println("Syntax error");
            System.out.println("Expected Syntax: INSERT INTO <table_name>(<columns>) VALUES (<values>);");
            return;
        }

        try {
            String tableName = insertTokens.get(2);
            if (tableName.trim().length() == 0) {
                System.out.println("ERROR: Table name cannot be empty");
                return;
            }
            // parsing logic
            if (tableName.indexOf("(") > -1)
                tableName = tableName.substring(0, tableName.indexOf("("));

            TableInfo tableInfo = new TableInfo(tableName);

            if (!tableInfo._tableExist) {
                System.out.println("ERROR: Table does not exist.");
                return;
            }

            ArrayList<String> columnTokens = new ArrayList<>(Arrays.asList(
                    queryString.substring(queryString.indexOf("(") + 1, queryString.indexOf(") values")).split(",")));

            // Column List validation
            for (String colToken : columnTokens) {
                if (!tableInfo._colNames.contains(colToken.trim())) {
                    System.out.println("ERROR: Invalid column : " + colToken.trim());
                    return;
                }
            }

            String valuesString = queryString.substring(queryString.indexOf("values") + 6, queryString.length() - 1);

            ArrayList<String> valueTokens = new ArrayList<>(Arrays
                    .asList(valuesString.substring(valuesString.indexOf("(") + 1, valuesString.length()).split(",")));

            // fill attributes to insert
            List<Field> attributeToInsert = new ArrayList<>();

            for (TableCol colInfo : tableInfo._colData) {
                boolean columnProvided = false;
                int i;
                for (i = 0; i < columnTokens.size(); i++) {
                    if (columnTokens.get(i).trim().equals(colInfo._columnName)) {
                        columnProvided = true;
                        try {
                            String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokens.get(i).trim().equals("null")) {
                                if (!colInfo._isNullable) {
                                    System.out.println("ERROR: Cannot Insert NULL into " + colInfo._columnName);
                                    return;
                                }
                                colInfo._type = Type.NULL;
                                value = value.toUpperCase();
                            }
                            Field attr = new Field(colInfo._type, value);
                            attributeToInsert.add(attr);
                            break;
                        } catch (Exception e) {
                            System.out.println("ERROR: Invalid data format for " + columnTokens.get(i) + " values: "
                                    + valueTokens.get(i));
                            return;
                        }
                    }
                }
                if (columnTokens.size() > i) {
                    columnTokens.remove(i);
                    valueTokens.remove(i);
                }

                if (!columnProvided) {
                    if (colInfo._isNullable)
                        attributeToInsert.add(new Field(Type.NULL, "NULL"));
                    else {
                        System.out.println("ERROR: Cannot Insert NULL into " + colInfo._columnName);
                        return;
                    }
                }
            }

            // insert attributes to the page
            RandomAccessFile dstTable = new RandomAccessFile(getTBLFilePath(tableName), "rw");
            int dstPageNo = BPlusTree.getPageNoForInsert(dstTable, tableInfo._rootPageNum);
            Page dstPage = new Page(dstPageNo, dstTable);

            int rowNo = dstPage.addTableRow(tableName, attributeToInsert);

            dstTable.close();
            if (rowNo != -1)
                System.out.println("Record Inserted");
            System.out.println();
        } catch (Exception e) {
            System.out.println("Error: unable to insert the record"+e);
        }
    }

    /**
     * Create new table
     *
     * param queryString is a String of the user input
     */
    public static void parseCreateTable(String createTableString) {
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
        // table and () check
        if (!createTableTokens.get(1).equals("table")) {
            System.out.println("Syntax Error");
            System.out.println("Expected Syntax: CREATE TABLE <table_name>(<col_name> <data_type> [not null] [unique] [primary key] );");
            return;
        }
        String tableName = createTableTokens.get(2);
        if (tableName.trim().length() == 0) {
            System.out.println("ERROR: Tablename cannot be empty");
            return;
        }
        try {
            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }

            List<TableCol> lstcolumnInformation = new ArrayList<>();
            ArrayList<String> columnTokens = new ArrayList<>(Arrays.asList(createTableString
                    .substring(createTableString.indexOf("(") + 1, createTableString.length() - 1).split(",")));

            short ordinalPosition = 1;

            for (String columnToken : columnTokens) {
                if (columnToken.equals(" "))
                    break;
                ArrayList<String> colInfoToken = new ArrayList<>(Arrays.asList(columnToken.trim().split(" ")));
                TableCol colInfo = new TableCol();
                colInfo._tableName = tableName;
                colInfo._columnName = colInfoToken.get(0);
                colInfo._isNullable = true;
                colInfo._type = Type.get(colInfoToken.get(1).toUpperCase());
                for (int i = 0; i < colInfoToken.size(); i++) {
                    if ((colInfoToken.get(i).equals("null"))) {
                        colInfo._isNullable = true;
                    }
                    else if (colInfoToken.get(i).equals("not") && (colInfoToken.get(i + 1).equals("null"))) {
                        colInfo._isNullable = false;
                        i++;
                    }
                    else if (colInfoToken.get(i).equals("unique")){
                        colInfo._isUnique = true;
                    }
                    else if (colInfoToken.get(i).equals("primary") && (colInfoToken.get(i + 1).equals("key"))) {
                        colInfo._isPrimaryKey = true;
                        colInfo._isUnique = true;
                        colInfo._isNullable = false;
                        i++;
                    }
                }
                colInfo._ordinalPosition = ordinalPosition++;
                lstcolumnInformation.add(colInfo);
            }

            // update meta data
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    getTBLFilePath(DavisBaseBinaryFile.tablesTable), "rw");
            TableInfo davisbaseTableMetaData = new TableInfo(DavisBaseBinaryFile.tablesTable);

            int pageNo = BPlusTree.getPageNoForInsert(davisbaseTablesCatalog, davisbaseTableMetaData._rootPageNum);

            Page page = new Page(pageNo, davisbaseTablesCatalog);
            // update davisbase_tables manually
            int rowNo = page.addTableRow(DavisBaseBinaryFile.tablesTable,
                    Arrays.asList(new Field(Type.TEXT, tableName),
                            new Field(Type.SMALLINT, "0"), new Field(Type.INT, "0"),
                            new Field(Type.INT, "0")));
            davisbaseTablesCatalog.close();

            if (rowNo == -1) {
                System.out.println("ERROR: Duplicate table Name");
                return;
            }

            RandomAccessFile tableFile = new RandomAccessFile(getTBLFilePath(tableName), "rw");

            Page.addNewPage(tableFile, PageType.tblLEAF, -1, -1);
            tableFile.close();

            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(getTBLFilePath(DavisBaseBinaryFile.columnsTable), "rw");
            TableInfo davisbaseColumnsMetaData = new TableInfo(DavisBaseBinaryFile.columnsTable);
            pageNo = BPlusTree.getPageNoForInsert(davisbaseColumnsCatalog, davisbaseColumnsMetaData._rootPageNum);

            Page page_1 = new Page(pageNo, davisbaseColumnsCatalog);

            for (TableCol column : lstcolumnInformation)
                page_1.addNewColumn(column);

            davisbaseColumnsCatalog.close();

            System.out.println("Table created");

        } catch (Exception e) {
            System.out.println("Error while creating Table");
            System.out.println(e.getMessage());
            parseDelete("delete from " + DavisBaseBinaryFile.tablesTable + " where table_name = '" + tableName + "' ");
            parseDelete("delete from " + DavisBaseBinaryFile.columnsTable + " where table_name = '" + tableName + "' ");
        }
    }

    /**
     * Delete records from table
     *
     * param queryString is a String of the user input
     */
    private static void parseDelete(String deleteTableString) {
        ArrayList<String> deleteTableTokens = new ArrayList<>(Arrays.asList(deleteTableString.split(" ")));

        String tableName = "";

        try {
            if (!deleteTableTokens.get(1).equals("from")) {
                System.out.println("Syntax Error");
                System.out.println("Expected Syntax: DELETE FROM <table_name> WHERE <condition>;");
                return;
            }

            tableName = deleteTableTokens.get(2);

            TableInfo metaData = new TableInfo(tableName);

            // get AND(1), OR(2)
            int whereConnect = 0;
            String condstring_1 = deleteTableString;
            String condstring_2 = "where ";
            if (deleteTableTokens.contains("and")) {
                condstring_1 = deleteTableString.substring(0,deleteTableString.indexOf("and"));
                condstring_2 += deleteTableString.substring(deleteTableString.indexOf("and")+4, deleteTableString.length());
                //out.println("cond1: " + condstring_1);
                //out.println("cond2: " + condstring_2);
                whereConnect = 1;
            }
            else if (deleteTableTokens.contains("or")){
                condstring_1 = deleteTableString.substring(0,deleteTableString.indexOf("or"));
                condstring_2 += deleteTableString.substring(deleteTableString.indexOf("or")+3, deleteTableString.length());
                whereConnect =2;
            }

            WhereCondition condition_1 = null;
            try {
                condition_1 = WhereCondition.extractConditionFromQuery(metaData, condstring_1);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                return;
            }

            WhereCondition condition_2 = null;
            if (whereConnect != 0) {
                try{
                    condition_2 = WhereCondition.extractConditionFromQuery(metaData, condstring_2);
                }catch (Exception e){
                    System.out.println(e.getMessage());
                    return;
                }
            }
            //////
            RandomAccessFile tblFile = new RandomAccessFile(getTBLFilePath(tableName), "rw");

            BPlusTree tree = new BPlusTree(metaData._rootPageNum, metaData._tableName, tblFile);
            int count = 0;
            for (int pageNo : tree._leavesNum) {
                short deleteCountPerPage = 0;
                Page page = new Page(pageNo, tblFile);
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
                    page.DeleteTBLRecord(tableName, Integer.valueOf(record._pageHeaderIndex - deleteCountPerPage).shortValue());
                    deleteCountPerPage++;
                    count++;
                }
            }

            System.out.println();
            tblFile.close();
            System.out.println(count + " record(s) deleted!");

        } catch (Exception e) {
            System.out.println("Error while deleting rows in table : " + tableName + " " + e);
        }

    }
}
