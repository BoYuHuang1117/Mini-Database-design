import java.util.Date;
import java.text.SimpleDateFormat;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Author: Bo-Yu Huang
 * Date: 7/25/20
 */

public class Field{
    public Type _type;
    public byte[] _byteAtt;
    public Byte[] _ByteAtt;
    public String _strValue;
    
    Field(Type type, byte[] value) throws Exception{
        _type = type;
        _byteAtt = value;
        
        try{
            switch(_type)
            {
                case NULL: 
                    _strValue = "NULL";
                    break;
                case TINYINT:
                    _strValue = Byte.valueOf(LoadByte.byteFromByteArray(_byteAtt)).toString();
                    break;
                case SMALLINT:
                    _strValue = Short.valueOf(LoadByte.shortFromByteArray(_byteAtt)).toString();
                    break;
                case INT:
                    _strValue = Integer.valueOf(LoadByte.intFromByteArray(_byteAtt)).toString();
                    break;
                case LONG:
                    _strValue = Long.valueOf(LoadByte.longFromByteArray(_byteAtt)).toString();
                    break;
                case FLOAT:
                    _strValue = Float.valueOf(LoadByte.floatFromByteArray(_byteAtt)).toString();
                    break;
                case DOUBLE:
                    _strValue = Double.valueOf(LoadByte.doubleFromByteArray(_byteAtt)).toString();
                    break;
                case YEAR:
                    _strValue = Integer.valueOf((int)LoadByte.byteFromByteArray(_byteAtt)+2000).toString();
                    break;
                case TIME:
                    int millisSinceMidnight = LoadByte.intFromByteArray(_byteAtt) % 86400000;
                    int seconds = millisSinceMidnight / 1000;
                    int hours = seconds / 3600;
                    int remHourSeconds = seconds % 3600;
                    int minutes = remHourSeconds / 60;
                    int remSeconds = remHourSeconds % 60;
                    _strValue = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
                    break;
                case DATETIME:
                    Date rawdatetime = new Date(LoadByte.longFromByteArray(_byteAtt));
                    _strValue = String.format("%02d", rawdatetime.getYear()+1900) + "-" + String.format("%02d", rawdatetime.getMonth()+1)
                        + "-" + String.format("%02d", rawdatetime.getDate()) + "_" + String.format("%02d", rawdatetime.getHours()) + ":"
                        + String.format("%02d", rawdatetime.getMinutes()) + ":" + String.format("%02d", rawdatetime.getSeconds());
                    break;
                case DATE:
                    Date rawdate = new Date(Long.valueOf(LoadByte.longFromByteArray(_byteAtt)));
                    _strValue = String.format("%02d", rawdate.getYear()+1900) + "-" + String.format("%02d", rawdate.getMonth()+1)
                        + "-" + String.format("%02d", rawdate.getDate());
                    break;
                case TEXT:
                    _strValue = new String(_byteAtt,UTF_8);
                    break;
            }
            _ByteAtt = LoadByte.byteToBytes(_byteAtt);
        } catch(Exception e) {
            throw new Exception("ERROR: Formatting exception",e);
        }
    }
    
    Field(Type type, String strVal) throws Exception {
        _type = type;
        _strValue = strVal;
        
        try {
            switch(_type)
            {
                case NULL: 
                    _byteAtt = null;
                    break;
                case TINYINT: 
                    _byteAtt = new byte[]{ Byte.parseByte(_strValue)}; 
                    break;
                case SMALLINT: 
                    _byteAtt = LoadByte.shortTobytes(Short.parseShort(_strValue));
                    break;
                case INT: 
                case TIME: 
                    _byteAtt = LoadByte.intTobytes(Integer.parseInt(_strValue));
                    break;
                case LONG: 
                    _byteAtt = LoadByte.longTobytes(Long.parseLong(_strValue));
                    break;
                case FLOAT: 
                    _byteAtt = LoadByte.floatTobytes(Float.parseFloat(_strValue));
                    break;
                case DOUBLE: 
                    _byteAtt = LoadByte.doubleTobytes(Double.parseDouble(_strValue));
                    break;
                case YEAR: 
                    _byteAtt = new byte[] { (byte) (Integer.parseInt(_strValue) - 2000) }; 
                    break;
                case DATETIME:
                    SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                    Date datetime = sdftime.parse(_strValue);
                    _byteAtt = LoadByte.longTobytes(datetime.getTime());
                    break;
                case DATE:
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = sdf.parse(_strValue);
                    _byteAtt = LoadByte.longTobytes(date.getTime());
                    break;
                case TEXT: 
                    _byteAtt = _strValue.getBytes(); 
                    break;
            }
            _ByteAtt = LoadByte.byteToBytes(_byteAtt);
        } catch (Exception e) {
            throw new Exception("ERROR: Cannot convert " + _strValue + " to " + _type.toString(),e);
        }
    }
}

enum Type{
    NULL((byte)0),
    TINYINT((byte)1),
    SMALLINT((byte)2),
    INT((byte)3),
    LONG((byte)4),
    FLOAT((byte)5),
    DOUBLE((byte)6),
    YEAR((byte)8),
    TIME((byte)9),
    DATETIME((byte)10),
    DATE((byte)11),
    TEXT((byte)12);
    @Override
    public String toString() {
        switch(this) {
            case NULL: return "NULL";
            case TINYINT: return "TINYINT";
            case SMALLINT: return "SMALLINT";
            case INT: return "INT";
            case LONG: return "LONG";
            case FLOAT: return "FLOAT";
            case DOUBLE: return "DOUBLE";
            case YEAR: return "YEAR";
            case TIME: return "TIME";
            case DATETIME: return "DATETIME";
            case DATE: return "DATE";
            case TEXT: return "TEXT";            
        }
        return "Can't defined type";
    }
    
    public byte _value;
    Type(byte value){ _value = value;}
    
    static HashMap<Byte,Type> byteToTypeMap = new HashMap<>();
    static HashMap<Byte, Integer> typeSizeMap = new HashMap<>();
    static HashMap<String, Type> stringToTypeMap = new HashMap<>();
    static HashMap<Type, Integer> typePrintMap = new HashMap<>();
    
    static {
        for (Type type : Type.values()){
            byteToTypeMap.put(type._value, type);
            stringToTypeMap.put(type.toString(), type);
            
            if (type == Type.NULL){
                typeSizeMap.put(type._value, 0);
                typePrintMap.put(type, 6);
            }
            else if (type == Type.TINYINT || type == Type.YEAR){
                typeSizeMap.put(type._value, 1);
                typePrintMap.put(type, 6);
            }
            else if (type == Type.SMALLINT){
                typeSizeMap.put(type._value, 2);
                typePrintMap.put(type, 8);
            }
            else if (type == Type.INT || type == Type.FLOAT || type == Type.TIME){
                typeSizeMap.put(type._value, 4);
                typePrintMap.put(type, 10);
            }
            else if (type == Type.LONG || type == Type.DOUBLE || type == Type.DATETIME || type == Type.DATE){
                typeSizeMap.put(type._value, 8);
                typePrintMap.put(type, 25);
            }
            else if (type == Type.TEXT){
                typePrintMap.put(type, 25);
            }
        }   
    }
    static Type get(byte value){
        if (value > 12)
            return Type.TEXT;
        return byteToTypeMap.get(value);
    }
    
    static Type get(String str){ return stringToTypeMap.get(str);}
    static int getTypeSize(byte value){
        if (get(value) == Type.TEXT)
            return value - 12;
        return typeSizeMap.get(value);
    }
    public int getPrintOffset() { return typePrintMap.get(this);}
}

class LoadByte{
    
    /* From-byte functions*/
    public static byte byteFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).get();
    }

    public static short shortFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    public static int intFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static long longFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static float floatFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }

    public static double doubleFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }
    
    static Byte[] byteToBytes(byte[] data){
        int len = (data == null ? 0 : data.length);
        Byte[] ans = new Byte[len];
        for (int i = 0; i < len; i++)
            ans[i] = data[i];
        
        return ans;
    }
    
    public static byte[] Bytestobytes(Byte[] data){
        if (data == null) System.out.println("Data is null");
        int len = (data == null ? 0 : data.length);
        byte[] result= new byte[len];
        for(int i=0;i<len;i++)
            result[i] = data[i];
        return result;
    }
    
    public static Byte[] shortToBytes(short data){
        return byteToBytes(ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array());
    }

    public static byte[] shortTobytes(short data){
        return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array();
    }
    
    public static Byte[] intToBytes(int data){
		return byteToBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array());
	}

    public static byte[] intTobytes(int data) {
		return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array();
	}
    
    public static byte[] longTobytes(long data) {
		return ByteBuffer.allocate(Long.BYTES).putLong(data).array();
	}

    public static byte[] floatTobytes(float data) {
		return (ByteBuffer.allocate(Float.BYTES).putFloat(data).array());
    }

    public static byte[] doubleTobytes(double data) {
		return (ByteBuffer.allocate(Double.BYTES).putDouble(data).array());
    }
}