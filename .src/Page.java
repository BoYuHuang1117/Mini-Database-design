import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Author: Bo-Yu Huang
 * Date: 7/26/20
 */

enum PageType{
    tblINTERIOR((byte)5),
    tblLEAF((byte)13);

    public byte _value;
    PageType(byte value){
        _value = value;
    }

    static HashMap<Byte, PageType> pageTypeHashMap= new HashMap<>(){{
        put((byte)5,PageType.tblINTERIOR);
        put((byte)13,PageType.tblLEAF);
    }};

    static PageType byteToPageType(byte value) { return pageTypeHashMap.get(value);}

    static PageType getPageType(RandomAccessFile file, int pageNum) throws IOException{
        try{
            file.seek(DavisBaseBinaryFile.pageSize * pageNum);
            Byte type = file.readByte();
            return byteToPageType(type);
        }catch (IOException e){
            System.out.println("ERROR: unable to obtain page type" + e.getMessage());
            throw e;
        }
    }
}

public class Page {
    RandomAccessFile _tblFile;

    // page header information
    int _pageNum;
    PageType _pageType;
    short _numCell;
    short _offsetForContent;
    int _NumOfRight;
    int _NumOfParent;

    // help class members for easy accessing
    HashMap<Integer, Integer> _leftChildrenMap; // <lastRowIdInPage, leftChildrenPageNum>
    List<TableRow> _records;
    long _pageStart;
    int _lastID;
    int _spaceLeft;
    HashMap<Integer, TableRow> _recordsMap;

    Page(int pageNum, RandomAccessFile tblFile){
        try {
            _tblFile = tblFile;
            // locate the corresponding page number in a huge table file
            _pageNum = pageNum;
            _pageStart = DavisBaseBinaryFile.pageSize * _pageNum;
            _tblFile.seek(_pageStart);
            _pageType = PageType.byteToPageType(_tblFile.readByte());
            _tblFile.seek(_pageStart+2);
            _numCell = _tblFile.readShort();
            _offsetForContent = _tblFile.readShort();
            _NumOfRight = _tblFile.readInt();
            _NumOfParent = _tblFile.readInt();
            _spaceLeft = _offsetForContent - 16 - _numCell*2;

            if (_pageType == PageType.tblLEAF)
                getPageRows();

            if (_pageType == PageType.tblINTERIOR) {
                try {
                    _leftChildrenMap = new HashMap<>();

                    int leftChildPageNo = 0;
                    int rowId = 0;
                    for (int i = 0; i < _numCell; i++) {
                        _tblFile.seek(_pageStart + 0x10 + (i * 2));
                        short cellStart = _tblFile.readShort();
                        if (cellStart == 0)//ignore deleted cells
                            continue;
                        _tblFile.seek(_pageStart + cellStart);

                        leftChildPageNo = _tblFile.readInt();
                        rowId = _tblFile.readInt();
                        _leftChildrenMap.put(rowId, leftChildPageNo);
                    }
                } catch(IOException e){
                    System.out.println("Error: unable to obtain all left children from the interior page " + e.getMessage());
                }
            }

        }catch(IOException e){
            System.out.println("Error: unable to read the page " + e.getMessage());
        }
    }

    public List<TableRow> getPageRows() {
        short payLoadSize = 0;
        byte noOfcolumns = 0;
        _records = new ArrayList<>();
        _recordsMap = new HashMap<>();
        try {
            for (short i = 0; i < _numCell; i++) {
                _tblFile.seek(_pageStart + 0x10 + (i *2) );
                short cellStart = _tblFile.readShort();
                if(cellStart == 0)
                    continue;
                _tblFile.seek(_pageStart + cellStart);

                payLoadSize = _tblFile.readShort();
                int rowId = _tblFile.readInt();
                noOfcolumns = _tblFile.readByte();

                // if the record was deleted, it won't appear in the 2xcell header.
                // But it can be found in the cellStartOffset, and it should be greater or equal to _numCell
                if(_lastID < rowId)
                    _lastID = rowId;

                byte[] colDatatypes = new byte[noOfcolumns];
                byte[] recordBody = new byte[payLoadSize - noOfcolumns - 1];

                _tblFile.read(colDatatypes);
                _tblFile.read(recordBody);

                TableRow record = new TableRow(i, cellStart, colDatatypes, recordBody, rowId);
                _records.add(record);
                _recordsMap.put(rowId, record);
            }
            // In case the last record is deleted, find the last row ID in current page
            //System.out.println("Current last ID: " + _lastID);
            _lastID = DavisBaseBinaryFile.getPageLastID(_tblFile, _pageNum);
            //System.out.println("New Current last ID: " + _lastID);
        } catch (IOException ex) {
            System.out.println("Error while filling records from the page " + ex.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return _records;
    }
    void copyPage(Page page){
        // _lastID should be consecutive, no need to copy
        _numCell = page._numCell;
        _records = page._records;
        _pageType = page._pageType;
        _NumOfParent = page._NumOfParent;
        _NumOfRight = page._NumOfRight;
        _recordsMap = page._recordsMap;
        _offsetForContent = page._offsetForContent;
        _pageNum = page._pageNum;
        _pageStart = page._pageStart;
        _leftChildrenMap = page._leftChildrenMap;
        _spaceLeft = page._spaceLeft;
    }

    static int addNewPage(RandomAccessFile file, PageType pagetype, int rightPage, int parentPage){
        try{
            int pageNum = Long.valueOf((file.length()/DavisBaseBinaryFile.pageSize)).intValue();
            file.setLength(file.length() + DavisBaseBinaryFile.pageSize);
            file.seek(DavisBaseBinaryFile.pageSize * pageNum);
            file.writeByte(pagetype._value);
            file.skipBytes(1);
            file.writeShort(0);
            file.writeShort((short)DavisBaseBinaryFile.pageSize);
            file.writeInt(rightPage);
            file.writeInt(parentPage);

            return pageNum;
        }catch(IOException e){
            System.out.println("ERROR: unable to add new page!" + e);
            return Integer.MIN_VALUE;
        }
    }

    public void updateRecord(TableRow record, int ordinalPos, Byte[] value) throws IOException{
        _tblFile.seek(_pageStart + record._recordOffset + 7);  // start of the "List of the column data type" in record header
        int loc = 0;
        for (int i = 0; i < ordinalPos; i++)
            loc += Type.getTypeSize(_tblFile.readByte());
        _tblFile.seek(_pageStart + record._recordOffset + 7 + record._colDatatypes.length + loc);
        _tblFile.write(LoadByte.Bytestobytes(value));
    }

    public void addNewColumn(TableCol colInfo) throws IOException{
        try {
            List<Field> meta_column = new ArrayList<>(Arrays.asList(new Field(Type.TEXT, colInfo._tableName),
                    new Field(Type.TEXT, colInfo._columnName),
                    new Field(Type.TEXT, colInfo._type.toString()),
                    new Field(Type.SMALLINT, colInfo._ordinalPosition.toString()),
                    new Field(Type.TEXT, colInfo._isNullable ? "YES" : "NO"),
                    colInfo._isPrimaryKey ?
                            new Field(Type.TEXT, "PRI") : new Field(Type.NULL, "NULL"),
                    new Field(Type.TEXT, colInfo._isUnique ? "YES" : "NO")));

            // update the catalog
            addTableRow(DavisBaseBinaryFile.columnsTable, meta_column);
        } catch (Exception e) {
            System.out.println("ERROR: unable to add new column: " + colInfo._columnName + e.getMessage());
        }
    }

    public int addTableRow(String tableName,List<Field> fields) throws IOException{
        List<Byte> colDataTypes = new ArrayList<>();
        List<Byte> recordBody = new ArrayList<>();

        TableInfo metaData  = null;
        if(DavisBaseBinaryFile.dataStoreInitialized){
            metaData = new TableInfo(tableName);
            if(!metaData.validateInsert(fields))
                return -1;
        }

        for(Field field : fields){
            //add value for the record body
            recordBody.addAll(Arrays.asList(field._ByteAtt));

            //Fill column Datatype for every attribute in the row
            if(field._type == Type.TEXT)
                colDataTypes.add(Integer.valueOf(Type.TEXT._value + (new String(field._strValue).length())).byteValue());
            else
                colDataTypes.add(field._type._value);
        }

        // guarantee the right-most page, but not promised to be the last row ID
        _lastID++;

        //calculate pay load size
        short payLoadSize = Integer.valueOf(recordBody.size() + colDataTypes.size() + 1).shortValue();

        //create record header
        List<Byte> recordHeader = new ArrayList<>();

        recordHeader.addAll(Arrays.asList(LoadByte.shortToBytes(payLoadSize)));  //payloadSize
        recordHeader.addAll(Arrays.asList(LoadByte.intToBytes(_lastID))); //last row_id
        recordHeader.add(Integer.valueOf(colDataTypes.size()).byteValue()); //number of columns
        recordHeader.addAll(colDataTypes); //column data types

        addNewPageRecord(recordHeader.toArray(new Byte[recordHeader.size()]), recordBody.toArray(new Byte[recordBody.size()]));

        if(DavisBaseBinaryFile.dataStoreInitialized){
            // update the catalog: both total number of row and row id will rise by one
            metaData._rowCount++;
            metaData._last_id++;
            metaData.updateCatalog();
        }
        return _lastID;
    }

    public void DeleteRecord(short recordIndex){
        // after deletion, the record won't exist in 2xcells section in page header, move everything after the deleted record one step ahead
        try{
            for (int i = recordIndex + 1; i < _numCell; i++){
                _tblFile.seek(_pageStart + 16 + i*2);
                short cellOffset = _tblFile.readShort();

                if (cellOffset == 0)
                    continue;
                _tblFile.seek(_pageStart + 16 + (i-1)*2);
                _tblFile.writeShort(cellOffset);
            }

            // if there is only one record (either the only first record or the last record)
            if (_numCell == 1){
                _tblFile.seek(_pageStart + 16);
                _tblFile.writeShort(0);
            }

            _tblFile.seek(_pageStart + 2);
            _tblFile.writeShort(--_numCell);
            // last_ID doesn't change
        }catch(IOException e){
            System.out.println("ERROR: unable to delete record!");
        }
    }

    public void DeleteTBLRecord(String tableName, short recordIndex){
        // delete certain record in certain page
        DeleteRecord(recordIndex);

        // update the catalog: total number of row would diminish, but row id won't
        TableInfo metaData = new TableInfo(tableName);
        metaData._rowCount--;
        metaData.updateCatalog();
        getPageRows();
    }

    private void addNewPageRecord(Byte[] recordHeader, Byte[] recordBody) throws IOException {
        //if there is no space in the current page
        if(recordHeader.length + recordBody.length + 4 > _spaceLeft){
            try{
                if(_pageType == PageType.tblLEAF || _pageType == PageType.tblINTERIOR)
                    handleTableOverFlow();
            }
            catch(IOException e){
                System.out.println("Error while handleTableOverFlow");
            }
        }
        // no need to left 2 byte space??
        short newCellStart = Integer.valueOf((_offsetForContent - recordBody.length - recordHeader.length - 2)).shortValue();
        _tblFile.seek(_pageNum * DavisBaseBinaryFile.pageSize + newCellStart);

        //record head
        _tblFile.write(LoadByte.Bytestobytes(recordHeader));

        //record body
        _tblFile.write(LoadByte.Bytestobytes(recordBody));
        // _numCell is different from lastRowId
        // update the page header
        _tblFile.seek(_pageStart + 0x10 + (_numCell * 2));
        _tblFile.writeShort(newCellStart);

        _offsetForContent = newCellStart;

        _tblFile.seek(_pageStart + 4); _tblFile.writeShort(_offsetForContent);

        _numCell++;
        _tblFile.seek(_pageStart + 2); _tblFile.writeShort(_numCell);

        _spaceLeft = _offsetForContent - 0x10 - (_numCell*2);
    }

    private void handleTableOverFlow() throws IOException {
        if(_pageType == PageType.tblLEAF) {
            //create a new leaf page
            int newRightLeafPageNo = addNewPage(_tblFile,_pageType,-1,-1);

            //if the current leaf page is root (the only page)
            if(_NumOfParent == -1){
                //create new parent page
                int newParentPageNo = addNewPage(_tblFile, PageType.tblINTERIOR, newRightLeafPageNo, -1);

                //(write in page) set the new leaf page as right sibling to the current page
                //set the newly created parent page as parent to the current page
                setRightPageNo(newRightLeafPageNo);
                setParent(newParentPageNo);

                //Add the current page as left child for the parent
                Page newParentPage = new Page(newParentPageNo, _tblFile);
                newParentPageNo = newParentPage.addLeftTableChild(_pageNum, _lastID);
                //(write in page) set the newly created leaf page as rightmost child of the parent
                newParentPage.setRightPageNo(newRightLeafPageNo);

                // add the newly created parent page as parent to newly created right page
                Page newLeafPage = new Page(newRightLeafPageNo, _tblFile);
                newLeafPage.setParent(newParentPageNo);

                // change the current page as newly created(empty) page
                copyPage(newLeafPage);
            }
            else
            {
                // if the leaf page is not root (multiple pages exist)
                // Add the current page as left child for the parent
                Page parentPage = new Page(_NumOfParent, _tblFile);
                _NumOfParent = parentPage.addLeftTableChild(_pageNum, _lastID);

                // add the newly created leaf page as rightmost child of the parent
                parentPage.setRightPageNo(newRightLeafPageNo);

                // set the new leaf page as right sibling to the current page
                setRightPageNo(newRightLeafPageNo);

                // add the parent page as parent to newly created right page
                Page newLeafPage = new Page(newRightLeafPageNo, _tblFile);
                newLeafPage.setParent(_NumOfParent);

                // change the current page as newly created(empty) page
                copyPage(newLeafPage);
            }
        }
        else if (_pageType == PageType.tblINTERIOR){
            // create a new left page on the right-most
            int newRightLeafPageNo = addNewPage(_tblFile, _pageType,-1,-1);

            //create new parent page
            int newParentPageNo = addNewPage(_tblFile, _pageType, newRightLeafPageNo, -1);

            //set the new leaf page as right sibling to the current page
            setRightPageNo(newRightLeafPageNo);

            //set the newly created parent page as parent to the current page
            setParent(newParentPageNo);

            //Add the current page as left child for the parent
            Page newParentPage = new Page(newParentPageNo, _tblFile);
            newParentPageNo = newParentPage.addLeftTableChild(_pageNum, _lastID);
            //add the newly created leaf page as rightmost child of the parent
            newParentPage.setRightPageNo(newRightLeafPageNo);

            //add the newly created parent page as parent to newly created right page
            Page newLeafPage = new Page(newRightLeafPageNo, _tblFile);
            newLeafPage.setParent(newParentPageNo);

            // change the current page as newly created(empty) page
            copyPage(newLeafPage);
        }
    }
    private int addLeftTableChild(int leftChildPageNo, int rowId) throws IOException{
        for (int lastRowIdInPage: _leftChildrenMap.keySet())
            if (lastRowIdInPage == rowId)
                return _pageNum;

        if(_pageType == PageType.tblINTERIOR){
            List<Byte> recordHeader= new ArrayList<>();
            List<Byte> recordBody= new ArrayList<>();

            recordHeader.addAll(Arrays.asList(LoadByte.intToBytes(leftChildPageNo)));
            recordBody.addAll(Arrays.asList(LoadByte.intToBytes(rowId)));

            // update the record in the interior page
            addNewPageRecord(recordHeader.toArray(new Byte[recordHeader.size()]), recordBody.toArray(new Byte[recordBody.size()]));
        }
        return _pageNum;
    }

    //sets the parentPageNo as parent for the current page
    public void setParent(int parentPageNo) throws IOException{
        _tblFile.seek(DavisBaseBinaryFile.pageSize * _pageNum + 0x0A);
        _tblFile.writeInt(parentPageNo);
        _NumOfParent = parentPageNo;
    }

    //sets the rightPageNo as rightPageNo (right sibling or right most child) for the current page
    public void setRightPageNo(int rightPageNo) throws IOException{
        _tblFile.seek(DavisBaseBinaryFile.pageSize * _pageNum + 0x06);
        _tblFile.writeInt(rightPageNo);
        _NumOfRight = rightPageNo;
    }
}
