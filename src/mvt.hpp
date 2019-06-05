#ifndef MVT_HPP
#define MVT_HPP


#include "sqlitedb.hpp"
#include <oqt/common.hpp>

typedef std::function<std::pair<double,double>(double,double,double)> transform_func;
transform_func make_transform(int64_t tx, int64_t ty, int64_t tz);
std::vector<blob> read_mvt_geometry(const transform_func& forward, int64_t np, oqt::uint64 gt, const std::vector<oqt::uint64>& cmds);

int64_t read_value_integer(const std::string& data);
double read_value_double(const std::string& data);
std::string read_value_string(const std::string& data);

struct mvt_feature {
    int64_t id;
    std::map<std::string,blob> properties;
    int64_t minzoom;
    std::vector<blob> geometries;
};
typedef std::map<std::string,std::vector<mvt_feature>> data_map;

std::vector<std::vector<std::pair<double,double>>> read_rings(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds, oqt::uint64 gt);
double ring_area(const std::vector<std::pair<double,double>>& r);
std::vector<blob> read_mvt_geometry_packed(std::string data);


data_map read_mvt_tile(const std::string& data_in, int64_t tx, int64_t ty, int64_t tz, bool gzipped, bool mvt_geoms);

#endif
