
#include "sqlite_funcs.hpp"
#include <string>
#include <iostream>
#include <regex>
#include <cmath>

#include "oqt/utils/pbf/fixedint.hpp"

/** sqlite funcs **/

static void hstore_contains(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<2) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_BLOB) {
        return sqlite3_result_int(context, 0);
    }
    if (sqlite3_value_type(argv[1])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    
    std::string data(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    
    std::string key(static_cast<const char *>(sqlite3_value_blob(argv[1])), sqlite3_value_bytes(argv[1]));
    
    //std::cout << "hstore_has([" << data.size() << " bytes], " << key << ")" << std::endl;
    
    if (data.size()<4) {
        std::cout << data << " too small" << std::endl;
        return sqlite3_result_error(context, "data format incorrect", -1);
    }
    size_t pos=0;
    size_t nt = oqt::read_int32(data, pos);
    for (size_t i=0; i < nt; i++) {
        if (pos >= data.size()) {
            std::cout << data << " key " << i << std::endl;
            return sqlite3_result_error(context, "data format incorrect", -1);
        }
        size_t kl = oqt::read_int32(data, pos);
        std::string tk=data.substr(pos, kl);
        //std::cout << "read " << tk << std::endl;
        if (tk == key) {
            //std::cout << "found!" << std::endl;
            return sqlite3_result_int(context, 1);
            
        }
        pos+=kl;
        
        if (pos >= data.size()) {
            std::cout << data << " val " << i << std::endl;
            return sqlite3_result_error(context, "data format incorrect", -1);
        }
        size_t vl = oqt::read_int32(data, pos);
        pos+=vl;
        
    }
    
    return sqlite3_result_int(context, 0);
}


static void hstore_contains_kv(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<3) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_BLOB) {
        return sqlite3_result_int(context, 0);
    }
    if (sqlite3_value_type(argv[1])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    
    if (sqlite3_value_type(argv[2])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    
    std::string data(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    
    std::string key(static_cast<const char *>(sqlite3_value_blob(argv[1])), sqlite3_value_bytes(argv[1]));
    std::string val(static_cast<const char *>(sqlite3_value_blob(argv[2])), sqlite3_value_bytes(argv[2]));
    
    //std::cout << "hstore_has([" << data.size() << " bytes], " << key << ")" << std::endl;
    
    if (data.size()<4) {
        std::cout << data << " too small" << std::endl;
        return sqlite3_result_error(context, "data format incorrect", -1);
    }
    size_t pos=0;
    size_t nt = oqt::read_int32(data, pos);
    for (size_t i=0; i < nt; i++) {
        if (pos >= data.size()) {
            std::cout << data << " key " << i << std::endl;
            return sqlite3_result_error(context, "data format incorrect", -1);
        }
        size_t kl = oqt::read_int32(data, pos);
        std::string tk=data.substr(pos, kl);
        //std::cout << "read " << tk << std::endl;
        bool found=(tk == key);
        pos+=kl;
        
        if (pos >= data.size()) {
            std::cout << data << " val " << i << std::endl;
            return sqlite3_result_error(context, "data format incorrect", -1);
        }
        
        size_t vl = oqt::read_int32(data, pos);
        if (found) {
            return sqlite3_result_int(context, (val==data.substr(pos,vl))?1:0);
        }
        
        pos+=vl;
        
    }
    
    return sqlite3_result_int(context, 0);
}

static void hstore_access(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<2) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_BLOB) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[1])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    
    std::string data(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    
    std::string key(static_cast<const char *>(sqlite3_value_blob(argv[1])), sqlite3_value_bytes(argv[1]));
    
    size_t pos=0;
    size_t nt = oqt::read_int32(data, pos);
    for (size_t i=0; i < nt; i++) {
        if (pos >= data.size()) { return sqlite3_result_error(context, "data format incorrect", -1); }
                        
        size_t kl = oqt::read_int32(data, pos);
        bool found = (data.substr(pos, kl) == key);
        pos+=kl;
        
        if (pos >= data.size()) { return sqlite3_result_error(context, "data format incorrect", -1); }
        size_t vl = oqt::read_int32(data, pos);
        if (found) {
            sqlite3_result_text(context, data.substr(pos,vl).c_str(), -1, SQLITE_TRANSIENT);
            return;
        }
        
        pos+=vl;
        
    }
    
    return sqlite3_result_null(context);
}

std::vector<std::string> split(const std::string& input, const std::string& regex) {
    // passing -1 as the submatch index parameter performs splitting
    std::regex re(regex);
    std::sregex_token_iterator
        first{input.begin(), input.end(), re, -1},
        last;
    return std::vector<std::string>(first, last);
}

static void string_to_array(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<2) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[1])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    
    std::string str(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    
    std::string sep(static_cast<const char *>(sqlite3_value_blob(argv[1])), sqlite3_value_bytes(argv[1]));
    
    
    auto str_split=split(str, sep);
    size_t l=(1+str_split.size())*4;
    for (const auto& s: str_split) {
        l+=s.size();
    }
    
    std::string out(l,'\0');
    size_t pos=0;
    
    pos = oqt::write_int32(out, pos, str_split.size());
    for (const auto& s: str_split) {
        pos = oqt::write_int32(out, pos, s.size());
        std::copy(s.begin(),s.end(),out.begin()+pos);
        pos+=s.size();
    }   
    sqlite3_result_blob(context, out.data(), out.size(), SQLITE_TRANSIENT);
        
    
}

static void array_to_string(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<2) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_BLOB) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[1])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    
    std::string data(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    
    std::string sep(static_cast<const char *>(sqlite3_value_blob(argv[1])), sqlite3_value_bytes(argv[1]));
    
    std::stringstream ss;
    bool first=true;
    
    
    size_t pos=0;
    if (data.size()<4) { return sqlite3_result_error(context, "too short", -1); }
    size_t nt = oqt::read_int32(data, pos);
    for (size_t i=0; i < nt; i++) {
        if (pos >= data.size()) { return sqlite3_result_error(context, "data format incorrect", -1); }
                        
        size_t kl = oqt::read_int32(data, pos);
        
        if (!first) {
            ss << sep;
        }
        first=false;
        ss << data.substr(pos,kl);
        pos+=kl;
    }
    
    return sqlite3_result_text(context, ss.str().c_str(), -1, SQLITE_TRANSIENT);
}

static void array_length(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<1) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_BLOB) {
        return sqlite3_result_null(context);
    }
    
    
    std::string data(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    if (data.size()<4) { return sqlite3_result_error(context, "too short", -1); }
    
    size_t pos=0;
    size_t nt = oqt::read_int32(data, pos);
    return sqlite3_result_int(context, nt);
}

static void max_entry_length(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<1) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_BLOB) {
        return sqlite3_result_null(context);
    }
    
    std::string data(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    
    if (data.size()<4) { return sqlite3_result_error(context, "too short", -1); }
    size_t pos=0;
    size_t nt = oqt::read_int32(data, pos);
    
    size_t max=0;
    
    for (size_t i=0; i < nt; i++) {
        if (pos >= data.size()) { return sqlite3_result_error(context, "data format incorrect", -1); }
                        
        size_t kl = oqt::read_int32(data, pos);
        
        if (kl>max) { max= kl; }
        
        pos+=kl;
    }
    return sqlite3_result_int(context, max);
}

static void regex_call(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<2) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[0])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    if (sqlite3_value_type(argv[1])!=SQLITE_TEXT) {
        return sqlite3_result_null(context);
    }
    
    std::string s(static_cast<const char *>(sqlite3_value_blob(argv[0])), sqlite3_value_bytes(argv[0]));
    
    std::string e_in(static_cast<const char *>(sqlite3_value_blob(argv[1])), sqlite3_value_bytes(argv[1]));
    
    std::regex e(e_in);

    if (std::regex_match (s,e)) {
        return sqlite3_result_int(context, 1);
    }
    return sqlite3_result_int(context, 0);
}


static double curr_pixel_area=1.0;

static void pixel_area(sqlite3_context *context, int argc, sqlite3_value **argv) {
    
    return sqlite3_result_double(context, curr_pixel_area);
}


double get_pixel_area() { return curr_pixel_area; }
void set_pixel_area(double d) { curr_pixel_area=d; }

static void power_call(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc<2) {
        return sqlite3_result_null(context);
    }
    
    int type0 = sqlite3_value_type(argv[0]);
    int type1 = sqlite3_value_type(argv[1]);
    if (! ((type0==SQLITE_INTEGER)  || (type0==SQLITE_FLOAT))) {
        return sqlite3_result_null(context);
    }
    if (! ((type1==SQLITE_INTEGER)  || (type1==SQLITE_FLOAT))) {
        return sqlite3_result_null(context);
    }
    
    double v0, v1;
    if (type0 == SQLITE_INTEGER) {
        v0 = sqlite3_value_int(argv[0]);
    } else {
        v0 = sqlite3_value_double(argv[0]);
    }
    
    if (type1 == SQLITE_INTEGER) {
        v1 = sqlite3_value_int(argv[1]);
    } else {
        v1 = sqlite3_value_double(argv[1]);
    }
    
    double result = pow(v0, v1);
    
    return sqlite3_result_double(context, result);
}


void register_functions(sqlite3* sqlite_conn) {
    

    sqlite3_create_function(sqlite_conn, "hstore_contains", 2, SQLITE_UTF8, nullptr, hstore_contains, nullptr, nullptr);
    sqlite3_create_function(sqlite_conn, "hstore_access", 2, SQLITE_UTF8, nullptr, hstore_access, nullptr, nullptr);
    sqlite3_create_function(sqlite_conn, "hstore_contains", 3, SQLITE_UTF8, nullptr, hstore_contains_kv, nullptr, nullptr);
    sqlite3_create_function(sqlite_conn, "string_to_array", 2, SQLITE_UTF8, nullptr, string_to_array, nullptr, nullptr);
    sqlite3_create_function(sqlite_conn, "array_to_string", 2, SQLITE_UTF8, nullptr, array_to_string, nullptr, nullptr);
    sqlite3_create_function(sqlite_conn, "array_length", 1, SQLITE_UTF8, nullptr, array_length, nullptr, nullptr);
    sqlite3_create_function(sqlite_conn, "max_entry_length", 1, SQLITE_UTF8, nullptr, max_entry_length, nullptr, nullptr);
    
    sqlite3_create_function(sqlite_conn, "regex_call", 2, SQLITE_UTF8, nullptr, regex_call, nullptr, nullptr);
    sqlite3_create_function(sqlite_conn, "pixel_area", 0, SQLITE_UTF8, nullptr, pixel_area, nullptr, nullptr);
    
    sqlite3_create_function(sqlite_conn, "power", 2, SQLITE_UTF8, nullptr, power_call, nullptr, nullptr);
    

}

