
#ifndef BINDELEMENT_HPP
#define BINDELEMENT_HPP

#include "sqlitedb.hpp"

#include "oqt/common.hpp"
#include "oqt/elements/block.hpp"
#include "oqt/geometry/utils.hpp"
#include "oqt/geometry/elements/point.hpp"
#include "oqt/geometry/elements/linestring.hpp"
#include "oqt/geometry/elements/simplepolygon.hpp"
#include "oqt/geometry/elements/complicatedpolygon.hpp"


namespace py=pybind11;

class BindElement2 {
    
    public:
        BindElement2(std::vector<std::string> keys, const std::string& _table_name);
        virtual ~BindElement2() {}
        
        const std::string& table_name() const{ return table_name_; }
        const std::string& table_create() const { return table_create_; }
        const std::string& table_insert() const { return table_insert_; }
        
        
        bool bind_id(sqlite3_stmt* curs, int64_t val) const;
        bool bind_tile(sqlite3_stmt* curs, int64_t val) const;
        bool bind_quadtree(sqlite3_stmt* curs, int64_t val) const;
        bool bind_part(sqlite3_stmt* curs, int64_t val) const;
        bool bind_tags(sqlite3_stmt* curs, const oqt::tagvector& tags) const;
        bool bind_layer(sqlite3_stmt* curs, int64_t val) const;
        bool bind_z_order(sqlite3_stmt* curs, int64_t val) const;
        bool bind_length(sqlite3_stmt* curs, double val) const;
        bool bind_minzoom(sqlite3_stmt* curs, int64_t val) const;
        bool bind_way_area(sqlite3_stmt* curs, double val) const;
        bool bind_way(sqlite3_stmt* curs, const std::string& val) const;
        
        bool bind_key_val(sqlite3_stmt* curs, const oqt::Tag& tg) const;
        
    private:
        std::string table_name_;
        int osm_id, tile, quadtree, part, tags, minzoom, layer, z_order, length, way_area, way;
        std::map<std::string,int> tag_keys;
        std::string table_create_, table_insert_;
        
        
};

size_t insert_element(std::shared_ptr<BindElement2> be, std::shared_ptr<SqliteDb> conn, oqt::ElementPtr ele, int64_t tile_qt);
size_t insert_python(std::shared_ptr<BindElement2> be, std::shared_ptr<SqliteDb> conn, const std::map<std::string,py::object>& props);
        


size_t default_get_tables(oqt::ElementPtr ele);
size_t extended_get_tables(oqt::ElementPtr ele);
std::vector<std::string> extended_table_names();


std::vector<size_t> insert_block(
    std::shared_ptr<SqliteDb> conn,
    std::vector<std::shared_ptr<BindElement2>> binds,
    oqt::PrimitiveBlockPtr bl, oqt::bbox bx, oqt::int64 minzoom,
    std::function<size_t(oqt::ElementPtr)> get_tables);


class mvt_feature;

size_t insert_mvt_feature(
    std::shared_ptr<BindElement2>& be, 
    std::shared_ptr<SqliteDb> conn,
    const mvt_feature& feat, size_t geom_idx);
    
std::string make_multigeom(const std::vector<blob>& geoms);

size_t insert_mvt_feature_allgeoms(
    std::shared_ptr<BindElement2>& be, 
    std::shared_ptr<SqliteDb> conn,
    const mvt_feature& feat);

size_t insert_mvt_tile(
    std::shared_ptr<SqliteDb> conn,
    
    std::function<std::shared_ptr<BindElement2>(std::string)> get_bind,
    const std::string& data, int64_t tx, int64_t ty, int64_t tz, bool gzipped,
    oqt::int64 minzoom, bool merge_geoms, bool mvt_geoms);
    
#endif
