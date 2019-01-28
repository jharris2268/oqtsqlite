#include "sqlitedb.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>


int bind_obj(sqlite3_stmt* curs, py::object obj, int col) {
    if (obj.is_none()) {
        return sqlite3_bind_null(curs, col);
    }
    try {
        auto d = py::cast<double>(obj);
        return sqlite3_bind_double(curs, col, d);
    } catch(...) {}
    
    try {
        auto i = py::cast<int64_t>(obj);
        return sqlite3_bind_int64(curs, col, i);
    } catch(...) {}
    
    try {
        auto s = py::cast<std::string>(obj);
        
        return sqlite3_bind_text(curs, col, s.data(), s.size(), SQLITE_TRANSIENT);
    } catch(...) {}

    try {
        auto b = py::cast<blob>(obj);
        
        return sqlite3_bind_blob(curs, col, b.s.data(), b.s.size(), SQLITE_TRANSIENT);
    } catch(...) {}
    
    throw std::domain_error("can't extract type from bind "+std::to_string(col));
}

bool call_bind_values(sqlite3_stmt* curs, py::object binds) {
    if (binds.is_none()) { return true; }
    
    auto lst = py::cast<py::list>(binds);
    int ln = py::len(lst);
    
    for (int i=0; i < ln; i++) {
        if (bind_obj(curs,lst[i],i+1)!=SQLITE_OK) {
            std::cout << "bind_obj(curs, " << py::str(lst[i]) << ", " << i+1 << ")" << std::endl;
            return false;
        }
    }
    
    return true;
}
    
    

        
SqliteDb::SqliteDb(const std::string& fn) {
    
    int r=0;
    
    if (fn.empty()) {
        r = sqlite3_open(":memory:", &sqlite_conn);
    } else {
        r = sqlite3_open(fn.c_str(), &sqlite_conn);
    }
    
    
    if (r!=SQLITE_OK) {
        sqlite3_close(sqlite_conn);
        call_error("open "+fn+" failed");
        
    }
    
    register_functions(sqlite_conn);
        
    
}


py::list SqliteDb::execute(const std::string& sql, py::object binds) {


    py::list result;
    prepare_step_finalize(sql, [&binds](sqlite3_stmt* c) { return call_bind_values(c,binds); }, make_python_result(result));
    
    return result;
    
    

}

mapnik::featureset_ptr SqliteDb::execute_featureset(const std::string& sql, mapnik::context_ptr ctx, py::object binds) {
    
    std::vector<mapnik::feature_ptr> feats;
    
    prepare_step_finalize(sql, [&binds](sqlite3_stmt* c) { return call_bind_values(c,binds); }, make_featurevector_result(feats, ctx));
    
    return make_featureset(std::move(feats));
    
}       

size_t SqliteDb::prepare_step_finalize(const std::string& sql, std::function<bool(sqlite3_stmt*)> bind_vals, std::function<bool(sqlite3_stmt*)> read_row) {
    sqlite3_stmt* curs;
    if (sqlite3_prepare_v2(sqlite_conn, sql.data(), sql.size(), &curs, nullptr) != SQLITE_OK) {
        call_error("prepare "+sql+" failed");
    }
    
    size_t i=0;
    
    if (bind_vals) {
        if (!bind_vals(curs)) {
            call_error("failed to bind values");
        }
        
    }
    
    bool cont=true;


    while (cont) {
    
        int r = sqlite3_step(curs);
        if (r==SQLITE_DONE) {
            cont=false;
        } else if (r==SQLITE_ROW) {
            if (read_row) { //i.e read_row is not nullptr
                
                if (!read_row(curs)) { //calling read_row fails
                    call_error("read row "+std::to_string(i)+" failed");
                }
            }
            i++;
        } else {
            call_error("step "+std::to_string(i)+" failed");
            cont=false;
        }    
    }
    
    sqlite3_finalize(curs);
    
    return i;
}

SqliteDb::~SqliteDb() {
    sqlite3_close(sqlite_conn);
}
void SqliteDb::call_error(const std::string& msg) {
    std::stringstream err;
    err << msg;
    err << sqlite3_errmsg(sqlite_conn);
    throw std::domain_error(err.str());
    
}
    
