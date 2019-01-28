#ifndef SQLITEDB_HPP
#define SQLITEDB_HPP


#include "sqlite_funcs.hpp"
#include "result.hpp"

struct blob {
    blob(const std::string& s_) : s(s_) {}
    std::string s;
};
int bind_obj(sqlite3_stmt* curs, py::object obj, int col);
bool call_bind_values(sqlite3_stmt* curs, py::object binds);

class SqliteDb {
    
    public:
        SqliteDb(const std::string& fn);
        virtual ~SqliteDb();
        
        py::list execute(const std::string& sql, py::object binds);
        mapnik::featureset_ptr execute_featureset(const std::string& sql, mapnik::context_ptr ctx, py::object binds);
        size_t prepare_step_finalize(const std::string& sql, std::function<bool(sqlite3_stmt*)> bind_vals, std::function<bool(sqlite3_stmt*)> read_row);
        void call_error(const std::string& msg);
    
    private:
        sqlite3* sqlite_conn;
        
        
        
};


#endif
