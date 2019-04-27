    
#include "bindelement.hpp"
#include "mvt.hpp"
#include "oqt/elements/geometry.hpp"
#include <sstream>
#include <iomanip>
#include <oqt/utils/pbf/fixedint.hpp>

std::string as_hex(const std::string& str) {
    std::stringstream ss;
    for (unsigned char c : str) {
        ss << std::hex << std::setfill('0') << std::setw(2) << (size_t) c;
    }

    return ss.str();

}

/*        
BindElement::BindElement(std::vector<std::string> keys, const std::string& _table_name)
        : table_name_(_table_name),
        osm_id(0), tile(0), quadtree(0), part(0), tags(0),
        minzoom(0), layer(0), z_order(0), length(0), way_area(0),
        way(0) {
    
    std::stringstream ss, ii;
    ss << "create table " << table_name_ << "(";
    ii << "insert into " << table_name_ << " values (";
    
    bool first=true;
    for (size_t i=0; i < keys.size(); i++) {
        const auto& k = keys[i];
        if (!first) {
            ss << ", ";
            ii << ", ";
            
        }
        first=false;
        
        if (k=="osm_id") {
            osm_id = i+1;
            ss << "osm_id integer"; ii << "?";
        }
        else if (k=="part") { part = i+1; ss << "part integer"; ii << "?"; }
        else if (k=="tile") { tile = i+1; ss << "tile integer"; ii << "?";}
        else if (k=="quadtree") { quadtree = i+1; ss << "quadtree integer"; ii << "?";}
        else if (k=="tags") { tags = i+1; ss << "tags blob"; ii << "?";}
        else if (k=="layer") { layer = i+1; ss << "layer integer"; ii << "?";}
        else if (k=="z_order") { z_order = i+1; ss << "z_order integer"; ii << "?";}
        else if (k=="minzoom") { minzoom = i+1; ss << "minzoom integer"; ii << "?";}
        else if (k=="length") { length = i+1; ss << "length float"; ii << "?";}
        else if (k=="way_area") { way_area = i+1; ss << "way_area integer"; ii << "?";}
        else if (k=="way") { way = i+1; ss << "way blob"; ii << "?";}
        else {
            tag_keys[k] = i+1;
            ss << '"' << k << '"';
            ii << "?";
        }
    }
    ss << ")"; ii << ")";
    table_create_ = ss.str();
    table_insert_ = ii.str();
    
}



size_t BindElement::insert_element(std::shared_ptr<SqliteDb> conn, oqt::ElementPtr ele, int64_t tile_qt) {
    
    
    if (ele->Type()==oqt::ElementType::Point) {
        auto pt = std::dynamic_pointer_cast<oqt::geometry::Point>(ele);
        conn->prepare_step_finalize(table_insert(), [this,pt,tile_qt](sqlite3_stmt* curs) { return this->call_point(pt,tile_qt,curs); },nullptr);
        return 1;
    } else if (ele->Type()==oqt::ElementType::Linestring) {
        auto ln = std::dynamic_pointer_cast<oqt::geometry::Linestring>(ele);
        conn->prepare_step_finalize(table_insert(), [this,ln,tile_qt](sqlite3_stmt* curs) { return this->call_line(ln,tile_qt,curs); },nullptr);
        return 1;
    } else if (ele->Type()==oqt::ElementType::SimplePolygon) {
        auto py = std::dynamic_pointer_cast<oqt::geometry::SimplePolygon>(ele);
        conn->prepare_step_finalize(table_insert(), [this,py,tile_qt](sqlite3_stmt* curs) { return this->call_simplepolygon(py,tile_qt,curs); },nullptr);
        return 1;
    } else if (ele->Type()==oqt::ElementType::ComplicatedPolygon) {
        auto py = std::dynamic_pointer_cast<oqt::geometry::ComplicatedPolygon>(ele);
        conn->prepare_step_finalize(table_insert(), [this,py,tile_qt](sqlite3_stmt* curs) { return this->call_complicatedpolygon(py,tile_qt,curs); },nullptr);
        return 1;
    }
    
    return 0;
}

size_t BindElement::insert_python(std::shared_ptr<SqliteDb> conn, const std::map<std::string,py::object>& props) {
    
    conn->prepare_step_finalize(table_insert(), [this,props](sqlite3_stmt* curs) { return this->call_python(props,curs); },nullptr);
    return 1;
    
}


bool BindElement::call_point(std::shared_ptr<oqt::geometry::Point> pt, int64_t tile_qt, sqlite3_stmt* curs) {
    
    if (osm_id!=0) {
        sqlite3_bind_int64(curs, osm_id, pt->Id());
    }
    
    if (!call_common(pt, tile_qt, curs)) {
        return false;
    }
    return true;
}

bool BindElement::call_line(std::shared_ptr<oqt::geometry::Linestring> ln, int64_t tile_qt, sqlite3_stmt* curs) {
    if (osm_id!=0) {
        if (sqlite3_bind_int64(curs, osm_id, ln->Id())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (!call_common(ln, tile_qt, curs)) {
        return false;
    }
 
    if (layer) { 
        if (sqlite3_bind_int64(curs, layer, ln->Layer())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (z_order) {
        if (sqlite3_bind_int64(curs, z_order, ln->ZOrder())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (length) { 
        if (sqlite3_bind_double(curs, length, ln->Length())!=SQLITE_OK) {
            return false;
        }
    }
    
    return true;
    
}

bool BindElement::call_simplepolygon(std::shared_ptr<oqt::geometry::SimplePolygon> py, int64_t tile_qt, sqlite3_stmt* curs) {
    if (osm_id!=0) {
        if (sqlite3_bind_int64(curs, osm_id, py->Id())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (!call_common(py, tile_qt, curs)) {
        return false;
    }
 
    if (layer) { 
        if (sqlite3_bind_int64(curs, layer, py->Layer())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (z_order) {
        if (sqlite3_bind_int64(curs, z_order, py->ZOrder())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (way_area) { 
        if (sqlite3_bind_double(curs, way_area, py->Area())!=SQLITE_OK) {
            return false;
        }
    }
    
    return true;
    
}

bool BindElement::call_complicatedpolygon(std::shared_ptr<oqt::geometry::ComplicatedPolygon> py, int64_t tile_qt, sqlite3_stmt* curs) {
    if (osm_id!=0) {
        if (sqlite3_bind_int64(curs, osm_id, -1*py->Id())!=SQLITE_OK) {
            return false;
        }
    }
    if (part!=0) {
        if (sqlite3_bind_int64(curs, part, py->Part())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (!call_common(py, tile_qt, curs)) {
        return false;
    }
 
    if (layer) { 
        if (sqlite3_bind_int64(curs, layer, py->Layer())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (z_order) {
        if (sqlite3_bind_int64(curs, z_order, py->ZOrder())!=SQLITE_OK) {
            return false;
        }
    }
    
    if (way_area) { 
        if (sqlite3_bind_double(curs, way_area, py->Area())!=SQLITE_OK) {
            return false;
        }
    }
    
    return true;

}


bool BindElement::call_common(std::shared_ptr<oqt::BaseGeometry> geom, int64_t tile_qt, sqlite3_stmt* curs) {
                
    if (tile!=0) {
        if (sqlite3_bind_int64(curs, tile, tile_qt)!=SQLITE_OK) {
            return false;
        }
    }
    
    if (quadtree!=0) {
        if (sqlite3_bind_int64(curs, quadtree, geom->Quadtree())!=SQLITE_OK) {
            return false;
        }
    }
    
    //std::vector<oqt::Tag> others;
    oqt::tagvector others;
    
    for (const auto& tg: geom->Tags()) {
        if (tag_keys.count(tg.key)) {
            if (sqlite3_bind_text(curs, tag_keys[tg.key], tg.val.data(), tg.val.size(), SQLITE_TRANSIENT)!=SQLITE_OK) {
                return false;
            }
        } else if (tags>0) {
            others.push_back(tg);
        }
    }
        
    if (!others.empty()) {
        auto s = oqt::geometry::pack_hstoretags_binary(others);
        //std::cout << geom->Id() << " " << others.size() << " [" << s.size() << " bytes: ] " << as_hex(s) << std::endl;
        if (sqlite3_bind_blob(curs, tags, s.data(), s.size(), SQLITE_TRANSIENT)!=SQLITE_OK) {
            return false;
        }
    }
    
    if (minzoom!=0) {
        if (geom->MinZoom()>=0) {
            if (sqlite3_bind_int64(curs, minzoom, geom->MinZoom())!=SQLITE_OK) {
                return false;
            }
        }
    }
    
    if (way!=0) {
        auto wkb = geom->Wkb(true, false);
        if (sqlite3_bind_blob(curs, way, wkb.data(), wkb.size(), SQLITE_TRANSIENT)!=SQLITE_OK) {
            return false;
        }
    }
    return true;
}

bool BindElement::call_python(const std::map<std::string,py::object>& props, sqlite3_stmt* curs) {

    oqt::tagvector others;
    for (const auto& pp: props) {
        int col=0;
        if (pp.first=="osm_id") { col=osm_id; }
        else if (pp.first == "tile") { col=tile; }
        else if (pp.first == "part") { col=part; }
        else if (pp.first == "quadtree") { col=quadtree; }
        else if (pp.first == "minzoom") { col=minzoom; }
        else if (pp.first == "layer") { col=layer; }
        else if (pp.first == "length") { col=length; }
        else if (pp.first == "way_area") {
            if (way_area == 0) {
                others.push_back(oqt::Tag("way_area", py::cast<std::string>(py::str(pp.second))));
            } else {
                col=way_area;
            }
        }
        else if (pp.first == "z_order") { col=z_order; }
        else if (pp.first == "way") { col=way; }
        else if (tag_keys.count(pp.first)) {
            col=tag_keys.at(pp.first);
        } else {
            others.push_back(oqt::Tag(pp.first, py::cast<std::string>(pp.second)));
        }
        
        if (col!=0) {
            if (bind_obj(curs, pp.second, col)!=SQLITE_OK) {
                return false;
            }
        }
    }
    
    if ((!others.empty()) && (tags!=0)) {
        auto s = oqt::geometry::pack_hstoretags_binary(others);
        //std::cout << geom->Id() << " " << others.size() << " [" << s.size() << " bytes: ] " << as_hex(s) << std::endl;
        if (sqlite3_bind_blob(curs, tags, s.data(), s.size(), SQLITE_TRANSIENT)!=SQLITE_OK) {
            return false;
        }
    }
    return true;
}
*/        
   
BindElement2::BindElement2(std::vector<std::string> keys, const std::string& _table_name)
        : table_name_(_table_name),
        osm_id(0), tile(0), quadtree(0), part(0), tags(0),
        minzoom(0), layer(0), z_order(0), length(0), way_area(0),
        way(0) {
    
    std::stringstream ss, ii;
    ss << "create table " << table_name_ << "(";
    ii << "insert into " << table_name_ << " values (";
    
    bool first=true;
    for (size_t i=0; i < keys.size(); i++) {
        const auto& k = keys[i];
        if (!first) {
            ss << ", ";
            ii << ", ";
            
        }
        first=false;
        
        if (k=="osm_id") {
            osm_id = i+1;
            ss << "osm_id integer"; ii << "?";
        }
        else if (k=="part") { part = i+1; ss << "part integer"; ii << "?"; }
        else if (k=="tile") { tile = i+1; ss << "tile integer"; ii << "?";}
        else if (k=="quadtree") { quadtree = i+1; ss << "quadtree integer"; ii << "?";}
        else if (k=="tags") { tags = i+1; ss << "tags blob"; ii << "?";}
        else if (k=="layer") { layer = i+1; ss << "layer integer"; ii << "?";}
        else if (k=="z_order") { z_order = i+1; ss << "z_order integer"; ii << "?";}
        else if (k=="minzoom") { minzoom = i+1; ss << "minzoom integer"; ii << "?";}
        else if (k=="length") { length = i+1; ss << "length float"; ii << "?";}
        else if (k=="way_area") { way_area = i+1; ss << "way_area integer"; ii << "?";}
        else if (k=="way") { way = i+1; ss << "way blob"; ii << "?";}
        else {
            tag_keys[k] = i+1;
            ss << '"' << k << '"';
            ii << "?";
        }
    }
    ss << ")"; ii << ")";
    table_create_ = ss.str();
    table_insert_ = ii.str();
    
}
bool BindElement2::bind_id(sqlite3_stmt* curs, int64_t val) const {
    if (osm_id==0) { return false; }
    
    if (sqlite3_bind_int64(curs, osm_id, val)!=SQLITE_OK) {
        throw std::domain_error("failed id");
    }
    return true;
}
    
bool BindElement2::bind_tile(sqlite3_stmt* curs, int64_t val) const {
    if (tile==0) { return false; }
    
    if (sqlite3_bind_int64(curs, tile, val)!=SQLITE_OK) {
        throw std::domain_error("failed tile");
    }
    return true;
}
bool BindElement2::bind_quadtree(sqlite3_stmt* curs, int64_t val) const {
    if (quadtree==0) { return false; }
    
    if (sqlite3_bind_int64(curs, quadtree, val)!=SQLITE_OK) {
        throw std::domain_error("failed quadtree");
    }
    return true;
}
bool BindElement2::bind_part(sqlite3_stmt* curs, int64_t val) const {
    if (part==0) { return false; }
    
    if (sqlite3_bind_int64(curs, part, val)!=SQLITE_OK) {
        throw std::domain_error("failed part");
    }
    return true;
}
bool BindElement2::bind_tags(sqlite3_stmt* curs, const oqt::tagvector& others) const {
    if (tags==0) { return false; }
    
    if (others.empty()) { return true; }
    auto s = oqt::geometry::pack_hstoretags_binary(others);
    
    if (sqlite3_bind_blob(curs, tags, s.data(), s.size(), SQLITE_TRANSIENT)!=SQLITE_OK) {
        throw std::domain_error("failed tags");
    }
    return true;
    
    
}
bool BindElement2::bind_layer(sqlite3_stmt* curs, int64_t val) const {
    if (layer==0) { return false; }
    
    if (sqlite3_bind_int64(curs, layer, val)!=SQLITE_OK) {
        throw std::domain_error("failed layer");
    }
    return true;
}

bool BindElement2::bind_minzoom(sqlite3_stmt* curs, int64_t val) const {
    if (layer==0) { return false; }
    
    if (sqlite3_bind_int64(curs, minzoom, val)!=SQLITE_OK) {
        throw std::domain_error("failed minzoom");
    }
    return true;
}

bool BindElement2::bind_z_order(sqlite3_stmt* curs, int64_t val) const {
    if (z_order==0) { return false; }
    
    if (sqlite3_bind_int64(curs, z_order, val)!=SQLITE_OK) {
        throw std::domain_error("failed z_order");
    }
    return true;
}
bool BindElement2::bind_length(sqlite3_stmt* curs, double val) const {
    if (length==0) { return false; }
    
    if (sqlite3_bind_double(curs, length, val)!=SQLITE_OK) {
        throw std::domain_error("failed length");
    }
    return true;
}
bool BindElement2::bind_way_area(sqlite3_stmt* curs, double val) const {
    if (way_area==0) { return false; }
    
    if (sqlite3_bind_double(curs, way_area, val)!=SQLITE_OK) {
        throw std::domain_error("failed way_area");
    }
    return true;
}
bool BindElement2::bind_way(sqlite3_stmt* curs, const std::string& val) const {
    if (way==0) { return false; }
    
    if (sqlite3_bind_blob(curs, way, val.data(), val.size(), SQLITE_TRANSIENT)!=SQLITE_OK) {
        throw std::domain_error("failed way");
    }
    return true;
    
    
}

bool BindElement2::bind_key_val(sqlite3_stmt* curs, const oqt::Tag& tg) const {
    if (tag_keys.count(tg.key)==0) {
        return false;
    }
    
    if (sqlite3_bind_text(curs, tag_keys.at(tg.key), tg.val.data(), tg.val.size(), SQLITE_TRANSIENT)!=SQLITE_OK) {
        throw std::domain_error("failed key_val "+tg.key+"='"+tg.val+"'");
    }
    return true;
}


size_t insert_python(std::shared_ptr<BindElement2> be, std::shared_ptr<SqliteDb> conn, const std::map<std::string,py::object>& props) {
    
    auto call_python = [&props, be](sqlite3_stmt* curs) {
        
        try {
            oqt::tagvector others;
            
            for (const auto& pp: props) {
            
                if (pp.first=="osm_id") { be->bind_id(curs, py::cast<int64_t>(pp.second)); }
                else if (pp.first == "tile") { be->bind_tile(curs, py::cast<int64_t>(pp.second)); }
                else if (pp.first == "part") { be->bind_part(curs, py::cast<int64_t>(pp.second));  }
                else if (pp.first == "quadtree") { be->bind_quadtree(curs, py::cast<int64_t>(pp.second)); }
                else if (pp.first == "minzoom") { be->bind_minzoom(curs, py::cast<int64_t>(pp.second));  }
                else if (pp.first == "layer") { be->bind_layer(curs, py::cast<int64_t>(pp.second)); }
                else if (pp.first == "z_order") { be->bind_z_order(curs, py::cast<int64_t>(pp.second)); }
                
                else if (pp.first == "length") { be->bind_length(curs, py::cast<double>(pp.second)); }
                else if (pp.first == "way_area") {
                    if (!be->bind_way_area(curs, py::cast<double>(pp.second))) {
                        others.push_back(oqt::Tag("way_area", py::cast<std::string>(py::str(pp.second))));
                    }
                }
                
                else if (pp.first == "way") { be->bind_way(curs, py::cast<blob>(pp.second).s);}
                
                else { 
                    oqt::Tag tg{pp.first, py::cast<std::string>(pp.second)};
                    if (!be->bind_key_val(curs, tg)) {
                        others.push_back(tg);
                    }
                }
            }
            
            be->bind_tags(curs, others);
            return true;
        } catch (std::exception& ex) {
            std::cout << "??" << ex.what() <<std::endl;
            return false;
        }
    };
    
    conn->prepare_step_finalize(be->table_insert(), call_python,nullptr);
    return 1;
    
}

namespace insert_element_internal {
void call_common(std::shared_ptr<BindElement2> be, sqlite3_stmt* curs, std::shared_ptr<oqt::BaseGeometry> geom, int64_t tile_qt, oqt::tagvector& others) {
               
    be->bind_tile(curs, tile_qt);
    if (geom->Quadtree()>=0) {
        be->bind_quadtree(curs, geom->Quadtree());
    }
    if (geom->MinZoom()>=0) {
        be->bind_minzoom(curs, geom->MinZoom());
    }
    
    
    for (const auto& tg: geom->Tags()) {
        if (!be->bind_key_val(curs, tg)) {
            others.push_back(tg);
        }
    }
        
    if (!others.empty()) {
        be->bind_tags(curs, others);
    }
    
    
    auto wkb = geom->Wkb(true, false);
    be->bind_way(curs, wkb);
    
}
void call_point(std::shared_ptr<BindElement2> be, sqlite3_stmt* curs, std::shared_ptr<oqt::geometry::Point> pt, int64_t tile_qt) {    
    be->bind_id(curs, pt->Id());
    oqt::tagvector others;
    call_common(be, curs, pt, tile_qt,others);
}

void call_line(std::shared_ptr<BindElement2> be, sqlite3_stmt* curs, std::shared_ptr<oqt::geometry::Linestring> ln, int64_t tile_qt) {
    be->bind_id(curs, ln->Id());
    
    oqt::tagvector others;
    
    if (!be->bind_layer(curs, ln->Layer())) { others.push_back(oqt::Tag{"layer",std::to_string(ln->Layer())}); }
    if (!be->bind_z_order(curs, ln->ZOrder())) { others.push_back(oqt::Tag{"z_order",std::to_string(ln->ZOrder())}); }
    if (!be->bind_way_area(curs, ln->Length())) { others.push_back(oqt::Tag{"length",std::to_string(ln->Length())}); }
    
    
    call_common(be, curs, ln, tile_qt,others);
    
}

void call_simplepolygon(std::shared_ptr<BindElement2> be, sqlite3_stmt* curs, std::shared_ptr<oqt::geometry::SimplePolygon> py, int64_t tile_qt) {
    be->bind_id(curs, py->Id());
    
    oqt::tagvector others;
    
    if (!be->bind_layer(curs, py->Layer())) { others.push_back(oqt::Tag{"layer",std::to_string(py->Layer())}); }
    if (!be->bind_z_order(curs, py->ZOrder())) { others.push_back(oqt::Tag{"z_order",std::to_string(py->ZOrder())}); }
    if (!be->bind_way_area(curs, py->Area())) { others.push_back(oqt::Tag{"way_area",std::to_string(py->Area())}); }
    call_common(be, curs, py, tile_qt, others);
    
}

void call_complicatedpolygon(std::shared_ptr<BindElement2> be, sqlite3_stmt* curs, std::shared_ptr<oqt::geometry::ComplicatedPolygon> py, int64_t tile_qt) {
    be->bind_id(curs, -1*py->Id());
    be->bind_part(curs, py->Part());
    
    oqt::tagvector others;
    
    if (!be->bind_layer(curs, py->Layer())) { others.push_back(oqt::Tag{"layer",std::to_string(py->Layer())}); }
    if (!be->bind_z_order(curs, py->ZOrder())) { others.push_back(oqt::Tag{"z_order",std::to_string(py->ZOrder())}); }
    if (!be->bind_way_area(curs, py->Area())) { others.push_back(oqt::Tag{"way_area",std::to_string(py->Area())}); }
    call_common(be, curs, py, tile_qt, others);
    
    

}

};


size_t insert_element(std::shared_ptr<BindElement2> be, std::shared_ptr<SqliteDb> conn, oqt::ElementPtr ele, int64_t tile_qt) {
    
    auto call_element = [be,ele,&tile_qt](sqlite3_stmt* curs) {
        try {
            if (ele->Type() == oqt::ElementType::Point) {
                auto pt=std::dynamic_pointer_cast<oqt::geometry::Point>(ele);
                if (!pt) { return false; }
                insert_element_internal::call_point(be,curs,pt,tile_qt);
            } else if (ele->Type() == oqt::ElementType::Linestring) {
                auto pt=std::dynamic_pointer_cast<oqt::geometry::Linestring>(ele);
                if (!pt) { return false; }
                insert_element_internal::call_line(be,curs,pt,tile_qt);
            } else if (ele->Type() == oqt::ElementType::SimplePolygon) {
                auto pt=std::dynamic_pointer_cast<oqt::geometry::SimplePolygon>(ele);
                if (!pt) { return false; }
                insert_element_internal::call_simplepolygon(be,curs,pt,tile_qt);
            } else if (ele->Type() == oqt::ElementType::ComplicatedPolygon) {
                auto pt=std::dynamic_pointer_cast<oqt::geometry::ComplicatedPolygon>(ele);
                if (!pt) { return false; }
                insert_element_internal::call_complicatedpolygon(be,curs,pt,tile_qt);
            } else {
                return false;
            }
        } catch (std::exception& ex) {
            std::cout << "??" << ex.what() << std::endl;
            return false;
        }
        return true;
    };  
    conn->prepare_step_finalize(be->table_insert(), call_element,nullptr);
    return 1;
    
}

size_t default_get_tables(oqt::ElementPtr ele) {
    if (ele->Type() == oqt::ElementType::Point) {
        return 1;
    }
    if (ele->Type() == oqt::ElementType::Linestring) {
        return 2;
    }
    if (ele->Type() == oqt::ElementType::SimplePolygon) {
        return 4;
    }
    if (ele->Type() == oqt::ElementType::ComplicatedPolygon) {
        return 4;
    }
    return 0;
}

bool is_building(oqt::ElementPtr ele) {
    for (const auto& tg: ele->Tags() ) {
        if (tg.key=="building") {
            return tg.val != "no";
        }
    }
    return false;
}

bool is_boundary(oqt::ElementPtr ele) {
    for (const auto& tg: ele->Tags() ) {
        if (tg.key=="boundary") {
            return tg.val != "no";
        }
    }
    return false;
}

size_t extended_get_tables(oqt::ElementPtr ele) {
    if (ele->Type() == oqt::ElementType::Point) {
        return 1;
    }
    if (ele->Type() == oqt::ElementType::Linestring) {
        auto ln = std::dynamic_pointer_cast<oqt::geometry::Linestring>(ele);
        if (ln && (ln->ZOrder()>0)) {
            return 8;
        }
        return 2;
    }
    if (ele->Type() == oqt::ElementType::SimplePolygon) {
        
        if (is_building(ele)) {
            return 16;
        }
        return 4;
    }
    if (ele->Type() == oqt::ElementType::ComplicatedPolygon) {
        if (is_boundary(ele)) {
            return 32;
        }
        
        return 4 | 64;
    }
    return 0;
}

std::vector<std::string> extended_table_names() {
    return {"point","line","polygon","highway","building","boundary","polypoint"};
}
/*
std::vector<size_t> insert_block(
    std::shared_ptr<SqliteDb> conn,
    std::vector<std::shared_ptr<BindElement>> binds,
    oqt::PrimitiveBlockPtr bl, oqt::bbox bx, oqt::int64 minzoom,
    std::function<size_t(oqt::ElementPtr)> get_tables) {
        
        
    if (!conn) { throw std::domain_error("no connection"); }
    if (!bl) { throw std::domain_error("no data"); }
    
    
    
    if (!get_tables) {
        get_tables = default_get_tables;
    }
    
    std::vector<size_t> cnt(binds.size(),0);
    for (auto ele: bl->Objects()) {
        auto geom=std::dynamic_pointer_cast<oqt::BaseGeometry>(ele);
        if (geom) {
            if ((minzoom>=0) && (geom->MinZoom() > minzoom)) {
                continue;
            }
            if (bx.minx<1800000000) {
                if (!oqt::overlaps(bx, geom->Bounds())) {
                    continue;
                }
            }
        }
        
        size_t tab = get_tables(ele);
        for (size_t i=0; i < binds.size(); i++) {
            if (tab & (1<<i)) {
                cnt.at(i) += binds[i]->insert_element(conn, ele, bl->Quadtree());
            }
        }
        
        
    }
    return cnt;
}*/


std::vector<size_t> insert_block(
    std::shared_ptr<SqliteDb> conn,
    std::vector<std::shared_ptr<BindElement2>> binds,
    oqt::PrimitiveBlockPtr bl, oqt::bbox bx, oqt::int64 minzoom,
    std::function<size_t(oqt::ElementPtr)> get_tables) {
    
    if (!conn) { throw std::domain_error("no connection"); }
    if (!bl) { throw std::domain_error("no data"); }
    
    
    
    if (!get_tables) {
        get_tables = default_get_tables;
    }
    
    
    
    auto check_element = [&minzoom, &bx](oqt::ElementPtr ele) {
        auto geom=std::dynamic_pointer_cast<oqt::BaseGeometry>(ele);
        
        if (!geom) {
            return false;
        }       
        
        if (geom->MinZoom() < 0) {
            return false;
        }
            
        if ((minzoom>=0) && (geom->MinZoom() > minzoom)) {
            return false;
        }
        if (bx.minx<1800000000) {
            if (!oqt::overlaps(bx, geom->Bounds())) {
                return false;
            }
        }
        return true;
    };
    
    std::vector<size_t> cnt(binds.size(),0);
    for (auto ele: bl->Objects()) {
        
        if (check_element(ele)) {
        
            size_t tab = get_tables(ele);
            for (size_t i=0; i < binds.size(); i++) {
                if (tab & (1<<i)) {
                    cnt.at(i) += insert_element(binds[i], conn, ele, bl->Quadtree());
                }
            }
        }
        
        
    }
    return cnt;
}
    



size_t insert_mvt_feature(
    std::shared_ptr<BindElement2>& be, 
    std::shared_ptr<SqliteDb> conn,
    const mvt_feature& feat, size_t geom_idx) {
    
    auto cb = [&be,&feat,geom_idx](sqlite3_stmt* curs) {
    
        be->bind_id(curs, feat.id);
        be->bind_minzoom(curs, feat.minzoom);
        
        oqt::tagvector others;
        for (const auto& kv: feat.properties) {
            if (kv.first == "part") { be->bind_part(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "quadtree") { be->bind_quadtree(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "tile") { be->bind_tile(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "layer") { be->bind_layer(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "z_order") { be->bind_z_order(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "length") { be->bind_length(curs, read_value_double(kv.second.s)); }
            else if (kv.first == "way_area") { be->bind_way_area(curs, read_value_double(kv.second.s)); }
            else {
                oqt::Tag tg(kv.first, read_value_string(kv.second.s));
                if (!be->bind_key_val(curs, tg)) {
                    others.push_back(tg);
                }
            }
        }
        if (!others.empty()) {
            be->bind_tags(curs, others);
        }
        
        be->bind_way(curs, feat.geometries.at(geom_idx).s);
        return true;
    };
    
    conn->prepare_step_finalize(be->table_insert(), cb,nullptr);
    return 1;
}

std::string make_multigeom(const std::vector<blob>& geoms) {
    
    if (geoms.size()==0) {
        return "\1\7\0\0\0\0\0\0\0";
    }
    if (geoms.size()==1) {
        return geoms[0].s;
    }
    
    size_t ln=9;
    char ty=0;
    for (const auto& g: geoms) {
        ln += g.s.size();
        if (ty==0) {
            ty = g.s[1];
        } else if (ty==4) {
            //pass
        } else {
            if (ty!=(g.s[1])) {
                ty=4;
            }
        }
    }
    
    std::string out(ln,'\0');
    out[0] = '\1';
    oqt::write_uint32_le(out, 1, ty+3);
    oqt::write_uint32_le(out, 5, geoms.size());
    size_t p=9;
    for (const auto& g: geoms) {
        std::copy(g.s.begin(),g.s.end(), out.begin()+p);
        p += g.s.size();
    }
    
    return out;
}
    
    

size_t insert_mvt_feature_allgeoms(
    std::shared_ptr<BindElement2>& be, 
    std::shared_ptr<SqliteDb> conn,
    const mvt_feature& feat) {
    
    auto cb = [&be,&feat](sqlite3_stmt* curs) {
    
        be->bind_id(curs, feat.id);
        be->bind_minzoom(curs, feat.minzoom);
        
        oqt::tagvector others;
        for (const auto& kv: feat.properties) {
            if (kv.first == "part") { be->bind_part(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "quadtree") { be->bind_quadtree(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "tile") { be->bind_tile(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "layer") { be->bind_layer(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "z_order") { be->bind_z_order(curs, read_value_integer(kv.second.s)); }
            else if (kv.first == "length") { be->bind_length(curs, read_value_double(kv.second.s)); }
            else if (kv.first == "way_area") { be->bind_way_area(curs, read_value_double(kv.second.s)); }
            else {
                oqt::Tag tg(kv.first, read_value_string(kv.second.s));
                if (!be->bind_key_val(curs, tg)) {
                    others.push_back(tg);
                }
            }
        }
        if (!others.empty()) {
            be->bind_tags(curs, others);
        }
        
        be->bind_way(curs, make_multigeom(feat.geometries));
        return true;
    };
    
    conn->prepare_step_finalize(be->table_insert(), cb,nullptr);
    return 1;
}


size_t insert_mvt_tile(
    std::shared_ptr<SqliteDb> conn,
    
    std::function<std::shared_ptr<BindElement2>(std::string)> get_bind,
    const std::string& data, int64_t tx, int64_t ty, int64_t tz, bool gzipped,
    oqt::int64 minzoom, bool merge_geoms, bool mvt_geoms) {
        
        
    if (!conn) { throw std::domain_error("no connection"); }
    if (!get_bind) { throw std::domain_error("not get_bind"); }
       
    
    size_t cnt=0;
    auto tables=read_mvt_tile(data,tx,ty,tz,gzipped, mvt_geoms);
    
    for (const auto& pp: tables) {
        std::shared_ptr<BindElement2> be = get_bind(pp.first);
        if (!be) { continue; }
        
        for (const auto& feat: pp.second) {
            if (merge_geoms || mvt_geoms) {
                cnt += insert_mvt_feature_allgeoms(be, conn, feat);
            } else {
                for (size_t i=0; i < feat.geometries.size(); i++) {
                    cnt+=insert_mvt_feature(be,conn,feat,i);
                }
            }
        }
    }
    return cnt;
}
