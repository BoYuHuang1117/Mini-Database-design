import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Author: Bo-Yu Huang
 * Date: 7/28/20
 */

public class BPlusTree {
    int _rootPageNum;
    String _tableName;
    RandomAccessFile _tblFile;
    List<Integer> _leavesNum;

    BPlusTree(int rootPageNum, String tableName, RandomAccessFile tblFile) throws IOException {
        _tableName = tableName;
        _tblFile = tblFile;
        _rootPageNum = rootPageNum;
        _leavesNum = getAllLeaves();
    }

    // This method does a traversal on the B+1 tree and returns the leaf pages in order
    public List<Integer> getAllLeaves() throws IOException {
        List<Integer> leafPages = new ArrayList<>();
        _tblFile.seek(_rootPageNum * DavisBaseBinaryFile.pageSize);
        // if root is leaf page read directly return one one, no traversal required
        PageType rootPageType = PageType.byteToPageType(_tblFile.readByte());
        if (rootPageType == PageType.tblLEAF) {
            if (!leafPages.contains(_rootPageNum))
                leafPages.add(_rootPageNum);
        } else
            addLeaves(_rootPageNum, leafPages);
        return leafPages;
    }

    // recursively adds leaves
    private void addLeaves(int interiorPageNo, List<Integer> leafPages) throws IOException {
        Page interiorPage = new Page(interiorPageNo, _tblFile);

        for (int lastRowIdInPage : interiorPage._leftChildrenMap.keySet()) {
            if (PageType.getPageType(_tblFile, interiorPage._leftChildrenMap.get(lastRowIdInPage)) == PageType.tblLEAF){
                if (!leafPages.contains(interiorPage._leftChildrenMap.get(lastRowIdInPage)))
                    leafPages.add(interiorPage._leftChildrenMap.get(lastRowIdInPage));
            }else
                addLeaves(interiorPage._leftChildrenMap.get(lastRowIdInPage), leafPages);
        }

        if (PageType.getPageType(_tblFile, interiorPage._NumOfRight) == PageType.tblLEAF){
            if (!leafPages.contains(interiorPage._NumOfRight))
                leafPages.add(interiorPage._NumOfRight);
        }else
            addLeaves(interiorPage._NumOfRight, leafPages);
    }

    // Returns the right most child page for inserting new records
    public static int getPageNoForInsert(RandomAccessFile file, int rootPageNo) {
        Page rootPage = new Page(rootPageNo, file);
        if (rootPage._pageType != PageType.tblLEAF)
            return getPageNoForInsert(file, rootPage._NumOfRight);
        else
            return rootPageNo;
    }
}
