
#include <mapnik/feature.hpp>
#include <mapnik/featureset.hpp>
#include <mapnik/wkb.hpp>

#include "result.hpp"
#include <set>

class PythonResult {
    public:
        PythonResult(py::list& result_) : result(result_), colnames_set(false) {}
        
        static std::function<bool(sqlite3_stmt*)> make(py::list& result) {
            auto pr = std::make_shared<PythonResult>(result);
            return [pr](sqlite3_stmt* curs) { return pr->call(curs); };
        }           
        
        bool call(sqlite3_stmt* curs) {
            
            int num_cols = sqlite3_column_count(curs);
            if (!colnames_set) {
                colnames.reserve(num_cols);
                for (int i=0; i < num_cols; i++) {
                    colnames.push_back(py::str(sqlite3_column_name(curs, i)));
                }
                colnames_set=true;
            }
            
            if (num_cols != (int) colnames.size()) {
                return false;
            }
            
            py::dict row;
            for (int i=0; i < num_cols; i++) {
                int ct = sqlite3_column_type(curs, i);
                if (ct == SQLITE_NULL) {
                    //pass
                } else if (ct==SQLITE_INTEGER) {
                    sqlite3_int64 v = sqlite3_column_int64(curs, i);
                    row[colnames[i]] = v;
                } else if (ct==SQLITE_FLOAT) {
                    double v = sqlite3_column_double(curs, i);
                    row[colnames[i]] = v;
                } else if ((ct==SQLITE_TEXT) || (ct==SQLITE_BLOB)) {
                    int l = sqlite3_column_bytes(curs, i);
                    const char *data = static_cast<const char *>(sqlite3_column_blob(curs, i));
                    row[colnames[i]] = py::bytes(data, l);
                }
                
            }
            result.append(row);
                
                    
            return true;
        }
    private:
        py::list& result;
        bool colnames_set;
        std::vector<py::str> colnames;
};          

std::function<bool(sqlite3_stmt*)> make_python_result(py::list& result) {
    auto pr = std::make_shared<PythonResult>(result);
    return [pr](sqlite3_stmt* curs) { return pr->call(curs); };
}           




class vector_featureset : public mapnik::Featureset {
    public:
        vector_featureset(std::vector<mapnik::feature_ptr>&& feats_) : feats(feats_), idx(0) {}
        
        mapnik::feature_ptr next() {
            if (idx<feats.size()) { return feats[idx++]; }
            return nullptr;
        }
    private:
        std::vector<mapnik::feature_ptr> feats;
        size_t idx;
};

mapnik::featureset_ptr make_featureset(std::vector<mapnik::feature_ptr>&& feats) {
    return std::make_shared<vector_featureset>(std::move(feats));
}

class FeatureVectorResult {
    public:
        FeatureVectorResult(std::vector<mapnik::feature_ptr>& result_, mapnik::context_ptr ctx_) : result(result_), ctx(ctx_), row_i(0), way_col(-1), id_col(-1),colnames_set(false) {}
        
        static std::function<bool(sqlite3_stmt*)> make(std::vector<mapnik::feature_ptr>& result, mapnik::context_ptr ctx) {
            auto fvr = std::make_shared<FeatureVectorResult>(result, ctx);
            return [fvr](sqlite3_stmt* curs) { return fvr->call(curs); };
        };
        
        
        bool call(sqlite3_stmt* curs) {
            
            int num_cols = sqlite3_column_count(curs);
            
            
            
            if (!colnames_set) {
                bool add_props=false;
                if (!ctx) {
                    
                    ctx=std::make_shared<mapnik::context_type>();
                    add_props=true;
                }
                
                for (int i=0; i < num_cols; i++) {
                    std::string cn = sqlite3_column_name(curs, i);
                    colnames.push_back(cn);
                    
                    if (cn=="osm_id") {
                        id_col=i;
                    } else if (cn=="way") {
                        way_col=i;
                    } else {
                        if (add_props) {
                            ctx->push(cn);
                        }
                    }
                }
                for (auto it=ctx->begin(); it!=ctx->end(); ++it) {
                    colnames_check.insert(it->first);
                }
                
                colnames_set=true;
            }
            
            if (num_cols != (int) colnames.size()) {
                return false;
            }
            
            
            int64_t id_ = row_i;
            if (id_col>=0) {
                id_ = sqlite3_column_int64(curs, id_col);
            }
            auto feat = std::make_shared<mapnik::feature_impl>(ctx, id_);
            
            if (way_col>=0) {
                if (sqlite3_column_type(curs,way_col)!=SQLITE_BLOB) {
                    //pass;
                } else {
                    int l = sqlite3_column_bytes(curs, way_col);
                    if (l==0) {
                        //pass;
                    } else {
                        const char *data = static_cast<const char *>(sqlite3_column_blob(curs, way_col));
                        feat->set_geometry(mapnik::geometry_utils::from_wkb(data, l));
                    }
                }
            }
            
            for (int i=0; i < num_cols; i++) {
                if ((i==id_col) || (i==way_col)) {
                    continue;
                }
                if (colnames_check.count(colnames[i])==0) {
                    continue;
                }
                
                int ct = sqlite3_column_type(curs, i);
                if (ct == SQLITE_NULL) {
                    //pass
                } else if (ct==SQLITE_INTEGER) {
                    sqlite3_int64 v = sqlite3_column_int64(curs, i);
                    feat->put(colnames[i], mapnik::value(v));
                } else if (ct==SQLITE_FLOAT) {
                    double v = sqlite3_column_double(curs, i);
                    feat->put(colnames[i], mapnik::value(v));
                } else if ((ct==SQLITE_TEXT) || (ct==SQLITE_BLOB)) {
                    int l = sqlite3_column_bytes(curs, i);
                    const char *data = static_cast<const char *>(sqlite3_column_blob(curs, i));
                    feat->put(colnames[i], mapnik::value_unicode_string::fromUTF8(std::string(data,l)));
                }
            }
            
            result.push_back(feat);
            row_i++;
                    
           
            return true;
        }
    private:
        std::vector<mapnik::feature_ptr>& result;
        mapnik::context_ptr ctx;
        size_t row_i;
        int way_col;
        int id_col;
        
        std::vector<std::string> colnames;
        bool colnames_set;
        std::set<std::string> colnames_check;
};

std::function<bool(sqlite3_stmt*)> make_featurevector_result(
    std::vector<mapnik::feature_ptr>& result, mapnik::context_ptr ctx) {
    auto fvr = std::make_shared<FeatureVectorResult>(result, ctx);
    return [fvr](sqlite3_stmt* curs) { return fvr->call(curs); };
};
