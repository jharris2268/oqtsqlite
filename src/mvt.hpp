#ifndef MVT_HPP
#define MVT_HPP


#include "sqlitedb.hpp"




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

data_map read_mvt_tile(const std::string& data_in, int64_t tx, int64_t ty, int64_t tz, bool gzipped);

#endif
