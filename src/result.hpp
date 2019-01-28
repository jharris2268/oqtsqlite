#ifndef RESULT_HPP
#define RESULT_HPP

#include <sqlite3.h>
#include <mapnik/feature.hpp>
#include <mapnik/featureset.hpp>
#include <mapnik/wkb.hpp>

#include <pybind11/pybind11.h>

namespace py=pybind11;

std::function<bool(sqlite3_stmt*)> make_python_result(py::list& result);

std::function<bool(sqlite3_stmt*)> make_featurevector_result(
    std::vector<mapnik::feature_ptr>& result, mapnik::context_ptr ctx);
mapnik::featureset_ptr make_featureset(std::vector<mapnik::feature_ptr>&& result);

#endif
