#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <pybind11/functional.h>

#include "sqlitedb.hpp"
#include <sqlite3.h>
#include "bindelement.hpp"
#include "mvt.hpp"

namespace py=pybind11;



py::object mvt_value_py(const std::string& s) {
    if ((s[0]==25) || (s[0]==21)) {
        return py::cast(read_value_double(s));
    }
    
    if ((s[0]>>3)==1) {
        return py::cast(read_value_string(s));
    }
    if ((s[0]>>3)==7) {
        return py::cast(s[1]!='\0');
    }
    return py::cast(read_value_integer(s));
    
}

py::dict mvt_feature_properties_py(const mvt_feature& feat) {
    
    py::dict result;
    for (const auto& kv: feat.properties) {
        result[py::cast(kv.first)] = mvt_value_py(kv.second.s);
    }
    return result;
    
    
}

py::object unpack_mapnik_geometry(const mapnik::geometry::geometry<double>& mg);

struct geom_visitor {
    py::object operator()(const mapnik::geometry::geometry_empty&) { return py::object(); }
    py::object operator()(const mapnik::geometry::point<double>& a) { return py::cast(a); }
    py::object operator()(const mapnik::geometry::line_string<double>& a) { return py::cast(a); }
    py::object operator()(const mapnik::geometry::polygon<double>& a) { return py::cast(a); }
    py::object operator()(const mapnik::geometry::multi_point<double>& a) { return py::cast(a); }
    py::object operator()(const mapnik::geometry::multi_line_string<double>& a)  { return py::cast(a); }
    py::object operator()(const mapnik::geometry::multi_polygon<double>& a) { return py::cast(a); }
    py::object operator()(const mapnik::geometry::geometry_collection<double>& a) {
        py::list res;
        for (const auto& g: a) {
            res.append(unpack_mapnik_geometry(g));
        }
        return res;
    }
};  

py::object unpack_mapnik_geometry(const mapnik::geometry::geometry<double>& mg) {
    
    geom_visitor vis;
    return mapnik::util::apply_visitor(vis, mg);
}
    


PYBIND11_DECLARE_HOLDER_TYPE(XX, std::shared_ptr<XX>);


PYBIND11_PLUGIN(_oqtsqlite) {
    py::module m("_oqtsqlite", "pybind11 example plugin");
    
    
    py::class_<sqlite3_wrap>(m,"sqlite3_wrap");
    
    py::class_<SqliteDb, std::shared_ptr<SqliteDb>>(m,"SqliteDb")
        .def(py::init<std::string>())
        .def("execute", &SqliteDb::execute, py::arg("sql"), py::arg("binds")=py::none())
        .def("execute_featureset", &SqliteDb::execute_featureset, py::arg("sql"), py::arg("ctx"), py::arg("binds")=py::none())
        .def("connection", &SqliteDb::connection)
    ;
    
    py::class_<blob>(m,"blob")
        .def(py::init<std::string>())
        .def("__call__", [](const blob& b) { return py::bytes(b.s.data(), b.s.size()); })
    ;
    /*
    py::class_<BindElement, std::shared_ptr<BindElement>>(m,"BindElement")
        .def(py::init<std::vector<std::string>, std::string>())
        .def_property_readonly("table_name", &BindElement::table_name)
        .def_property_readonly("table_insert", &BindElement::table_insert)
        .def_property_readonly("table_create", &BindElement::table_create)
        .def("insert_element", &BindElement::insert_element)
        .def("insert_python", &BindElement::insert_python)
    ;
    */
    py::class_<BindElement2, std::shared_ptr<BindElement2>>(m,"BindElement2")
        .def(py::init<std::vector<std::string>, std::string>())
        .def_property_readonly("table_name", &BindElement2::table_name)
        .def_property_readonly("table_insert", &BindElement2::table_insert)
        .def_property_readonly("table_create", &BindElement2::table_create)
        
    ;
    
    
    m.def("insert_python", &insert_python);
    m.def("insert_element", &insert_element);
    
    m.def("insert_block", &insert_block);
    m.def("set_pixel_area", &set_pixel_area);
    m.def("get_pixel_area", &get_pixel_area);
    
    m.def("default_get_tables", &default_get_tables);
    m.def("extended_get_tables", &extended_get_tables);
    m.def("extended_table_names", &extended_table_names);
    
    
    m.def("insert_mvt_feature", &insert_mvt_feature);
    m.def("insert_mvt_feature_allgeoms", &insert_mvt_feature_allgeoms);
    m.def("insert_mvt_tile", &insert_mvt_tile);
    
    py::class_<mvt_feature>(m, "mvt_feature")
        .def(py::init<int64_t,std::map<std::string,blob>,int64_t,std::vector<blob>>())
        .def_readonly("id", &mvt_feature::id)
        //.def_readonly("properties", &mvt_feature::properties)
        .def_property_readonly("properties", &mvt_feature_properties_py)
        .def_readonly("minzoom", &mvt_feature::minzoom)
        .def_readonly("geometries", &mvt_feature::geometries)
    ;
    m.def("make_multigeom", [](const mvt_feature& f) { return blob(make_multigeom(f.geometries)); });
    
    m.def("read_value_integer",[](const blob& b) { return read_value_integer(b.s); });
    m.def("read_value_double",[](const blob& b) { return read_value_double(b.s); });
    m.def("read_value_string",[](const blob& b) { return read_value_string(b.s); });
    
    m.def("read_mvt_tile", &read_mvt_tile);
    
    m.def("read_value", mvt_value_py);
    m.def("read_mvt_geometry_packed", &read_mvt_geometry_packed);
    m.def("read_mvt_geometry", &read_mvt_geometry);
    m.def("make_transform", &make_transform);
    //m.def("make_mapnik_geometry_from_mvt", [](const std::string& s) -> py::object { return unpack_mapnik_geometry(make_mapnik_geometry_from_mvt(s)); });
    m.def("read_rings", &read_rings);
    m.def("ring_area", ring_area);
    return m.ptr();
}

