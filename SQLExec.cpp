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
Indices *SQLExec::indices = nullptr;

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
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
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

/**
 * Deconstructor for QueryResult
 *
 * destory objects
 *
 * @param record_id  record to delete
 */
QueryResult::~QueryResult() {
    if(column_names != nullptr){
        delete column_names;
    }
    if(column_attributes != nullptr){
        delete column_attributes;
    }
    if(rows != nullptr){
        for(ValueDict *row : *rows){
            delete row;
        }
        delete rows;
    }
}


/**
 * exectute the specific statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if(tables == nullptr){
        tables = new Tables();
    }
    if(indices == nullptr){
        indices = new Indices();
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

/**
 * parse column name and column attribute from Column Definition
 * @param col  pointer to the Column Definition
 * @param column_name  var address for column name
 * @param column_attribute var address for column attributes
 */
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

/**
 * exectute the create table/index statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

/**
 * exectute the create/index statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
	Identifier table_name = statement->tableName;

    if(!table_exist(table_name)){
        throw SQLExecError(table_name + " not exist， can't build index on it");
    }

	DbRelation& table = SQLExec::tables->get_table(table_name);
	const ColumnNames& table_columns = table.get_column_names();
	for (auto const& col_name : *statement->indexColumns) {
		if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end()) {
			throw SQLExecError(string("column '" + string(col_name) + "' does not exist"));
		}
	}

	ValueDict row;
	row["table_name"] = Value(table_name);
	row["index_name"] = Value(index_name);
	row["index_type"] = Value(statement->indexType);
	row["is_unique"] = Value(string(statement->indexType) == "BTREE");
	int seq = 0;
	Handles inHandles;
    DbIndex* index = nullptr;
	try {
		for (auto const &col_name : *statement->indexColumns) {
			row["seq_in_index"] = Value(++seq);
			row["column_name"] = Value(col_name);
			inHandles.push_back(SQLExec::indices->insert(&row));
		}
		index = &(indices->get_index(table_name, index_name));
		index->create();
	}
	catch (...) {
        if(index != nullptr){
            index->drop();
        }
		try {
			for (auto const &handle : inHandles) {
				SQLExec::indices->del(handle);
			}
		}
		catch (...) {}
		throw;
	}
	return new QueryResult("created index " + index_name);
}

/**
 * exectute the create table statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;

    // insert new table into _table
    ValueDict row;
    row["table_name"] = Value(table_name);
    Handle table_handle = tables->insert(&row);

    Handles col_handles;
    DbRelation *column_table = nullptr;
    try{
        column_table = &(tables->get_table(Columns::TABLE_NAME));
        for (ColumnDefinition *col : *statement->columns) {
            Identifier column_name;
            ColumnAttribute column_attribute;
            column_definition(col, column_name, column_attribute);
            ValueDict row;
            row["table_name"] = Value(table_name);
            row["column_name"] = Value(column_name);
            row["data_type"] = Value(column_attribute.get_data_type() == ColumnAttribute::INT?"INT":"TEXT");
            col_handles.push_back(column_table->insert(&row));
        }
        // creat new table & cache new table
        DbRelation& table = tables->get_table(table_name);
        if (statement->ifNotExists)
            table.create_if_not_exists();
        else
            table.create();
    } catch (exception& e){
        if(column_table != nullptr){
            try{
                for(auto const &handle : col_handles){
                    column_table->del(handle);
                }
            } catch (...) {}
        }
        tables->del(table_handle);
        throw;
    }
    return new QueryResult("created table " + table_name);
}

/**
 * check if the table exists
 * @param table_name  name of the table you want to check
 */
bool SQLExec::table_exist(Identifier table_name){
    QueryResult *query_result = show_tables();
    bool res = false;
    for(auto const &row : *(query_result->get_rows())){
        if(row->at("table_name") == Value(table_name)){
            res = true;
            break;
        }
    }
    delete query_result;
    return res;
}

/**
 * exectute the drop table/index statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::drop(const DropStatement *statement){
    switch(statement->type){
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            throw SQLExecError("Only DROP TABLE and DROP INDEX are implemented");
    }
}

/**
 * exectute the drop table statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if(table_name == Tables::TABLE_NAME){
        throw SQLExecError("can't drop _tables");
    }
    if(table_name == Columns::TABLE_NAME){
        throw SQLExecError("can't drop _columns");
    }
    if(table_name == Indices::TABLE_NAME){
        throw SQLExecError("can't drop _indices");
    }
    if(!table_exist(table_name)){
        throw SQLExecError(table_name + " not exist");
    }

    // delete indices of this table
    IndexNames index_names = indices->get_index_names(table_name);
    for(Identifier index_name : index_names){
        drop_index(table_name, index_name);
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
    return new QueryResult("dropped " + table_name);
}


/**
 * exectute the drop index statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::drop_index(const DropStatement *statement){
    Identifier index_name = statement->indexName;
	Identifier table_name = statement->name;
    drop_index(table_name, index_name);
    return new QueryResult("drop index " + index_name);
}


/**
 * drop index
 * @param table_name  the table name of the index
 * @param index_name  the index name
 */
void SQLExec::drop_index(Identifier table_name, Identifier index_name){
    DbIndex& index = indices->get_index(table_name, index_name);
    // delete physical index
    index.drop();
    // delete records from _indices
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = indices->select(&where);
    for(Handle handle : *handles){
        indices->del(handle);
    }
    delete handles;
}


/**
 * exectute the show table/index statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type){
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            return new QueryResult("Not implemented");
    }
}


/**
 * exectute the show index statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    Identifier table_name = statement->tableName;
    if(!table_exist(table_name)){
        throw SQLExecError("table " + table_name + " doesn't exist");
    }

    ColumnNames *col_names = new ColumnNames();
    ColumnAttributes *col_attrs = new ColumnAttributes();
    tables->get_columns(Indices::TABLE_NAME, *col_names, *col_attrs);
    
    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *handles = indices->select(&where);
    ValueDicts *rows = new ValueDicts();
    for(Handle handle : *handles){
        ValueDict *row = indices->project(handle, col_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(col_names, col_attrs, rows,
        "successfully fetch " + to_string(rows->size()) + " rows");
}


/**
 * exectute the show table statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *col_names = new ColumnNames();
    ColumnAttributes *col_attrs = new ColumnAttributes();
    tables->get_columns(Tables::TABLE_NAME, *col_names, *col_attrs);

    Handles *handles = tables->select();
    ValueDicts *rows = new ValueDicts();
    for(Handle handle : *handles){
        ValueDict *row = tables->project(handle, col_names);
        Value name = row->at("table_name");
        if(name != Value(Tables::TABLE_NAME) && name != Value(Columns::TABLE_NAME) && name != Value(Indices::TABLE_NAME)){
            rows->push_back(row);
        } else {
            delete row;
        }
    }
    delete handles;
    return new QueryResult(col_names, col_attrs, rows, 
        "successfully fetch " + to_string(rows->size()) + " tables");
}

/**
 * exectute the show column statement
 * @param statement  pointer to the statement
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    ColumnNames *col_names = new ColumnNames();
    ColumnAttributes *col_attrs = new ColumnAttributes();
    tables->get_columns(Columns::TABLE_NAME, *col_names, *col_attrs);

    ValueDicts *rows = new ValueDicts();
    DbRelation &column = tables->get_table(Columns::TABLE_NAME);
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = column.select(&where);
    for(Handle handle : *handles){
        ValueDict *row = column.project(handle, col_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(col_names, col_attrs, rows, 
        "successfully fetch " + to_string(rows->size()) + " rows");
}
