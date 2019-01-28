#ifndef SQLITE_FUNCS
#define SQLITE_FUNCS

#include <sqlite3.h>

double get_pixel_area();
void set_pixel_area(double d);

void register_functions(sqlite3* sqlite_conn);

#endif
