
#include <mapnik/feature.hpp>
#include <mapnik/featureset.hpp>
#include <mapnik/wkb.hpp>

#include "result.hpp"
#include <set>
#include "mvt.hpp"
#include <oqt/utils/pbf/protobuf.hpp>
#include <oqt/utils/pbf/packedint.hpp>

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


mapnik::geometry::line_string<double> make_line_string(const std::vector<std::pair<double,double>>& coords) {
    mapnik::geometry::line_string<double> line(coords.size());
    for (size_t i=0; i < coords.size(); i++) {
        line[i] = mapnik::geometry::point<double>(coords[i].first,coords[i].second);
    }
    return line;
};

mapnik::geometry::linear_ring<double> make_ring(const std::vector<std::pair<double,double>>& coords) {
    mapnik::geometry::linear_ring<double> line(coords.size());
    for (size_t i=0; i < coords.size(); i++) {
        line[i] = mapnik::geometry::point<double>(coords[i].first,coords[i].second);
    }
    return line;
};

mapnik::geometry::geometry<double> make_mapnik_geometry_from_mvt(const std::string& data) {
    
    
    mapnik::geometry::geometry_collection<double> ll;
    try {
        auto geoms = read_mvt_geometry_packed(data);
    
        
        
        if (geoms.size()==1) {
            return mapnik::geometry_utils::from_wkb(geoms[0].s.data(), geoms[0].s.size());
        }
        
        
        for (const auto& g: geoms) {
            ll.push_back(mapnik::geometry_utils::from_wkb(g.s.data(), g.s.size()));
        }
    } catch (...) {}
    
    return ll;      
    
    
    /*
    
    
    size_t pos=0;
    auto tg = oqt::read_pbf_tag(data,pos);
    
    size_t gt=0;
    std::vector<oqt::uint64> cmds;
    size_t np=0;
    std::vector<oqt::uint64> tile_xyz;
    
        
    for ( ;  tg.tag>0; tg = oqt::read_pbf_tag(data,pos)) {
        if (tg.tag==3) { gt = tg.value; }
        if (tg.tag==4) { cmds = oqt::read_packed_int(tg.data); }
        if (tg.tag==5) { np = tg.value; }
        if (tg.tag==6) { tile_xyz = oqt::read_packed_int(tg.data); }
    }
    
    transform_func forward;
    if (tile_xyz.size()==3) {
        forward = make_transform(tile_xyz[0], tile_xyz[1], tile_xyz[2]);
    } else {
        forward = [](double x, double y, double npi) { return std::make_pair(x*256./npi, 256.0-y*256./npi); };
    }
    
    auto rings = read_rings(forward, np, cmds, gt);
    
    if (rings.size()==0) {
        return mapnik::geometry::geometry_collection<double>();
    }
    
    if (gt==1) {
        if (rings.size()==1) {
            return mapnik::geometry::point<double>(rings[0][0].first,rings[0][0].second);
        } else {
            mapnik::geometry::multi_point<double> multi;
            multi.reserve(rings.size());
            for (const auto& r: rings) {
                multi.push_back(mapnik::geometry::point<double>(r[0].first, r[0].second));
            }
            return multi;
        }
    } else if (gt==2) {
        if (rings.size()==1) {
            return make_line_string(rings[0]);
        } else {
            mapnik::geometry::multi_line_string<double> multi;
            multi.reserve(rings.size());
            for (const auto& r: rings) {
                multi.push_back(make_line_string(r));
            }
            return multi;
        }
    } else if (gt==3) {
        mapnik::geometry::multi_polygon<double> multi;
        mapnik::geometry::polygon<double> poly;
        for (const auto& r: rings) {
            if (ring_area(r)<0) {
                poly.push_back(make_ring(r));
            } else {
                if (!poly.empty()) {
                    multi.push_back(poly);
                    poly = mapnik::geometry::polygon<double>();
                }
                poly.push_back(make_ring(r));
            }
        }
        if (!poly.empty()) {
            multi.push_back(poly);
        }
        
        if (multi.size()==1) {
            return multi[0];
        }
        return multi;
    }
    return mapnik::geometry::geometry_collection<double>();*/
}            
        


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
                        
                        if (data[0] == '\x18') {
                            feat->set_geometry( make_mapnik_geometry_from_mvt(std::string(data, l)) );
                        } else {
                        
                            feat->set_geometry(mapnik::geometry_utils::from_wkb(data, l));
                        }
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
