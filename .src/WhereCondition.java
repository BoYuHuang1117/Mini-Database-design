import java.util.ArrayList;
import java.util.Arrays;

/**
 * Author: Bo-Yu Huang
 * Date: 7/25/20
 */

public class WhereCondition{
    // Condition_and_OperatorType
    String _columnName;
    OperatorType _operator;
    String _comparedValue;
    boolean _negation;
    int _columnOrdinal;
    Type _type;

    WhereCondition(Type dataType) { _type = dataType;}
    public void setConditionValue(String conditionValue) {
        _comparedValue = conditionValue;
        _comparedValue = _comparedValue.replace("'", "");
        _comparedValue = _comparedValue.replace("\"", "");
    }

    public void setOperator(String operator) { _operator = getOpType(operator);}

    public static int compare(String value1, String value2, Type dType) {
        if (dType == Type.TEXT)
            return value1.toLowerCase().compareTo(value2);
        else if (dType == Type.NULL) {
            if (value1.equals(value2))
                return 0;
            else if (value1.toLowerCase().equals("null"))
                return -1;
            else
                return 1;
        } else {
            return Long.valueOf(Long.parseLong(value1) - Long.parseLong(value2)).intValue();
        }
    }

    private boolean doOperationOnDifference(OperatorType operation,int difference){
        switch (operation) {
            case LESSOREQUAL:
                return difference <= 0;
            case GREATEROREQUAL:
                return difference >= 0;
            case NOTEQUAL:
                return difference != 0;
            case LESS:
                return difference < 0;
            case GREATER:
                return difference > 0;
            case EQUAL:
                return difference == 0;
            default:
                return false;
        }
    }

    private boolean doStringCompare(String currentValue, OperatorType operation) {
        return doOperationOnDifference(operation,currentValue.toLowerCase().compareTo(_comparedValue));
    }

    // Does comparison on currentValue with the comparison value
    public boolean checkCondition(String currentValue) {
        OperatorType operation = getOperation();
        if(currentValue.toLowerCase().equals("null") || _comparedValue.toLowerCase().equals("null"))
            return doOperationOnDifference(operation,compare(currentValue,_comparedValue,Type.NULL));
        if (_type == Type.TEXT || _type == Type.NULL)
            return doStringCompare(currentValue, operation);
        else if (_type == Type.DOUBLE || _type == Type.FLOAT){
            switch (operation) {
                case LESSOREQUAL:
                    return Double.parseDouble(currentValue) <= Double.parseDouble(_comparedValue);
                case GREATEROREQUAL:
                    return Double.parseDouble(currentValue) >= Double.parseDouble(_comparedValue);
                case NOTEQUAL:
                    return Double.parseDouble(currentValue) != Double.parseDouble(_comparedValue);
                case LESS:
                    return Double.parseDouble(currentValue) < Double.parseDouble(_comparedValue);
                case GREATER:
                    return Double.parseDouble(currentValue) > Double.parseDouble(_comparedValue);
                case EQUAL:
                    return Double.parseDouble(currentValue) == Double.parseDouble(_comparedValue);
                default:
                    return false;
            }
        }
        else {
            switch (operation) {
                case LESSOREQUAL:
                    return Long.parseLong(currentValue) <= Long.parseLong(_comparedValue);
                case GREATEROREQUAL:
                    return Long.parseLong(currentValue) >= Long.parseLong(_comparedValue);
                case NOTEQUAL:
                    return Long.parseLong(currentValue) != Long.parseLong(_comparedValue);
                case LESS:
                    return Long.parseLong(currentValue) < Long.parseLong(_comparedValue);
                case GREATER:
                    return Long.parseLong(currentValue) > Long.parseLong(_comparedValue);
                case EQUAL:
                    return Long.parseLong(currentValue) == Long.parseLong(_comparedValue);
                default:
                    return false;
            }
        }
    }

    public OperatorType getOperation() {
        if (!_negation)
            return _operator;
        else {
            switch (_operator) {
                case LESSOREQUAL:
                    return OperatorType.GREATER;
                case GREATEROREQUAL:
                    return OperatorType.LESS;
                case NOTEQUAL:
                    return OperatorType.EQUAL;
                case LESS:
                    return OperatorType.GREATEROREQUAL;
                case GREATER:
                    return OperatorType.LESSOREQUAL;
                case EQUAL:
                    return OperatorType.NOTEQUAL;
                default:
                    System.out.println("ERROR: Invalid operator \"" + _operator + "\"");
                    return OperatorType.INVALID;
            }
        }
    }

    static WhereCondition extractConditionFromQuery(TableInfo tableInfo, String query) throws Exception {
        if (query.contains("where")) {
            WhereCondition condition = new WhereCondition(Type.TEXT);
            String whereClause = query.substring(query.indexOf("where") + 6, query.length());
            ArrayList<String> whereClauseTokens = new ArrayList<>(Arrays.asList(whereClause.split(" ")));

            // WHERE NOT column operator value
            if (whereClauseTokens.get(0).equalsIgnoreCase("not")) {
                condition._negation = true;
            }

            for (int i = 0; i < WhereCondition.supportedOperators.length; i++) {
                if (whereClause.contains(WhereCondition.supportedOperators[i])) {
                    whereClauseTokens = new ArrayList<>(Arrays.asList(whereClause.split(WhereCondition.supportedOperators[i])));
                    condition.setOperator(WhereCondition.supportedOperators[i]);
                    condition.setConditionValue(whereClauseTokens.get(1).trim());
                    condition._columnName = whereClauseTokens.get(0).trim();
                    break;
                }
            }

            if (tableInfo._tableExist && tableInfo.checkColumnExists(new ArrayList<>(Arrays.asList(condition._columnName)))) {
                condition._columnOrdinal = tableInfo._colNames.indexOf(condition._columnName);
                condition._type = tableInfo._colData.get(condition._columnOrdinal)._type;

                if(condition._type != Type.TEXT && condition._type != Type.NULL) {
                    try {
                        if (condition._type == Type.DOUBLE || condition._type == Type.FLOAT)
                            Double.parseDouble(condition._comparedValue);
                        else
                            Long.parseLong(condition._comparedValue);
                    } catch (Exception e) {
                        throw new Exception("ERROR: Invalid Comparison " + e);
                    }
                }
            } else {
                throw new Exception("ERROR: Invalid Table/Column : " + tableInfo._tableName + " . " + condition._columnName);
            }
            return condition;
        } else
            return null;
    }

    static String[] supportedOperators = { "<=", ">=", "<>", ">", "<", "=" };

    // Converts the operator string from the user input to OperatorType
    static OperatorType getOpType(String strOperator) {
        switch (strOperator) {
            case ">":
                return OperatorType.GREATER;
            case "<":
                return OperatorType.LESS;
            case "=":
                return OperatorType.EQUAL;
            case ">=":
                return OperatorType.GREATEROREQUAL;
            case "<=":
                return OperatorType.LESSOREQUAL;
            case "<>":
                return OperatorType.NOTEQUAL;
            default:
                System.out.println("ERROR: Invalid operator \"" + strOperator + "\"");
                return OperatorType.INVALID;
        }
    }
}

enum OperatorType{
    LESS,
    EQUAL,
    GREATER,
    LESSOREQUAL,
    GREATEROREQUAL,
    NOTEQUAL,
    INVALID;
}