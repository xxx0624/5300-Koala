/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2020"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    // FIXME
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if(tables == nullptr){
        tables = new Tables();
    }
    
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type){
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("not supported data type");
    }
}

// TODO: exceptions
QueryResult *SQLExec::create(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    DbRelation &column_table = tables->get_table(Columns::TABLE_NAME);
    for (ColumnDefinition *col : *statement->columns) {
        Identifier column_name;
        ColumnAttribute column_attribute;
        column_definition(col, column_name, column_attribute);
        //column_names.push_back(column_name);
        //column_attributes.push_back(column_attribute);
        ValueDict row;
        row["table_name"] = Value(table_name);
        row["column_name"] = Value(column_name);
        row["data_type"] = Value(column_attribute.get_data_type() == ColumnAttribute::INT?"INT":"TEXT");
        column_table.insert(&row);
    }
    // insert new table into _table
    ValueDict row;
    row["table_name"] = Value(table_name);
    tables->insert(&row);
    // creat new table & cache new table
    DbRelation& table = tables->get_table(table_name);
    table.create_if_not_exists();
    return new QueryResult("created " + table_name);
}

bool SQLExec::table_exist(Identifier table_name){
    QueryResult *query_result = show_tables();
    ValueDicts *rows = query_result->get_rows();
    bool res = false;
    for(auto const &row : *rows){
        if(row->at("table_name") == Value(table_name)){
            res = true;
            break;
        }
    }
    delete query_result;
    delete rows;
    return res;
}

QueryResult *SQLExec::drop(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if(table_name == Tables::TABLE_NAME){
        return new QueryResult("can't drop _tables");
    }
    if(table_name == Columns::TABLE_NAME){
        return new QueryResult("can't drop _columns");
    }
    if(!table_exist(table_name)){
        return new QueryResult(table_name + " not exist");
    }

    DbRelation &table = tables->get_table(table_name);
    DbRelation &column = tables->get_table(Columns::TABLE_NAME);
    
    ValueDict where;
    where["table_name"] = Value(table_name);
    // delete table from _columns
    Handles *handles = column.select(&where);
    for(Handle handle : *handles){
        column.del(handle);
    }
    delete handles;
    // delete table from DB
    table.drop();
    // delete table from _tables
    handles = tables->select(&where);
    for(Handle handle : *handles){
        tables->del(handle);
        break;// Delete only one table
    }
    delete handles;
    return new QueryResult("drop " + table_name);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *col_names = new ColumnNames();
    ColumnAttributes *col_attrs = new ColumnAttributes();
    ValueDicts *rows = new ValueDicts();
    tables->get_columns(Tables::TABLE_NAME, *col_names, *col_attrs);
    Handles *handles = tables->select();
    for(Handle handle : *handles){
        ValueDict *row = tables->project(handle);
        Value name = row->at("table_name");
        if(name != Value(Tables::TABLE_NAME) && name != Value(Columns::TABLE_NAME)){
            rows->push_back(row);
        }
    }
    delete handles;
    return new QueryResult(col_names, col_attrs, rows, 
        "successfully fetch " + to_string(rows->size()) + " tables");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}
