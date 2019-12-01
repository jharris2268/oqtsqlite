


#include <oqt/utils/compress.hpp>
#include <oqt/utils/pbf/protobuf.hpp>
#include <oqt/utils/pbf/fixedint.hpp>


#include "mvt.hpp"
using namespace std::string_literals;



double read_float(const std::string& data, size_t pos) {
    union {
        float f;
        char sc[4];
    };
    
    sc[0] = data[pos];
    sc[1] = data[pos+1];
    sc[2] = data[pos+2];
    sc[3] = data[pos+3];
    return f;
}

double read_double(const std::string& data, size_t pos) {
    union {
        double d;
        char sc[8];
    };
    
    sc[0] = data[pos];
    sc[1] = data[pos+1];
    sc[2] = data[pos+2];
    sc[3] = data[pos+3];
    sc[4] = data[pos+4];
    sc[5] = data[pos+5];
    sc[6] = data[pos+6];
    sc[7] = data[pos+7];
    return d;
}




int64_t read_value_integer(const std::string& data) {
    if ((data[0]==25) && (data.size()==9)) {
        return std::round(read_double(data, 1));
    }
    
    if ((data[0]==21) && (data.size()==5)) {
        return std::round(read_float(data, 1));
    }
    size_t pos=0;
    auto tg=oqt::read_pbf_tag(data,pos);
    
    if (tg.tag==4) { return tg.value; }
    if (tg.tag==5) { return (int64_t) tg.value; }
    if (tg.tag==6) { return oqt::un_zig_zag(tg.value); }
    if (tg.tag==7) { return (int64_t) tg.value; }
    return 0;
}

double read_value_double(const std::string& data) {
    
    
    if ((data[0]==25) && (data.size()==9)) {
        return read_double(data, 1);
    }
    
    if ((data[0]==21) && (data.size()==5)) {
        return read_float(data, 1);
    }
    size_t pos=0;
    auto tg=oqt::read_pbf_tag(data,pos);
    if (tg.tag==4) { return tg.value; }
    if (tg.tag==5) { return (int64_t) tg.value; }
    if (tg.tag==6) { return oqt::un_zig_zag(tg.value); }

    return 0.0;
}

std::string read_value_string(const std::string& data) {
    
    if ((data[0]==25) && (data.size()==9)) {
        return std::to_string(read_double(data, 1));
    }
    if ((data[0]==21) && (data.size()==5)) {
        return std::to_string(read_float(data, 1));
    }
    
    size_t pos=0;
    auto tg=oqt::read_pbf_tag(data,pos);
    if (pos != data.size()) { throw std::domain_error("value with more than one tag???"); }
    
    if (tg.tag==1) { return tg.data; }
    
    if (tg.tag==4) { return std::to_string(tg.value); }
    if (tg.tag==5) { return std::to_string((int64_t) tg.value); }
    if (tg.tag==6) { return std::to_string(oqt::un_zig_zag(tg.value)); }
    if (tg.tag==7) { return (tg.value==1) ? "t" : "f"; }
    
    return "";
}

struct dedelta {
    dedelta() : d(0) {}
    
    int64_t operator()(oqt::uint64 v) {
        d += oqt::un_zig_zag(v);
        return d;
    }
    int64_t d;
};
/*
size_t add_point(std::string& s, size_t pos, const transform_func& forward, int64_t np, int64_t x, int64_t y) {
    auto xy = forward(x, y, np);
    pos = oqt::write_double_le(s, pos, xy.first);
    return oqt::write_double_le(s, pos, xy.second);
}

size_t read_points_int(std::vector<std::string>& points, const transform_func& forward, int64_t np, dedelta& x, dedelta& y,const std::vector<oqt::uint64>& cmds, size_t pos) {

    if ((cmds[pos]&7)!=1) { throw std::domain_error("not a point: first cmd not a moveto"); }
    size_t npts = cmds[pos] >> 3;    
        
    pos++;
    for (size_t i=0; i < npts; i++) {
        std::string s(21, '\0');
        s[0] = '\1';
        s[1] = '\1';
        add_point(s, 5, forward, np, x(cmds[pos]), y(cmds[pos+1]));
        pos+=2;
        
        points.push_back(s);
    }
    return pos;
}

std::vector<blob> read_point(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds) {
    
    
    std::vector<std::string> points;
    dedelta x,y;
    size_t pos=0;
    while (pos < cmds.size()) {
        pos= read_points_int(points, forward, np, x, y, cmds, pos);
    }
    
    std::vector<blob> res;
    for (const auto& p: points) { res.push_back(blob(p)); }
    return res;
}
*/

typedef std::pair<double,double> pt;

std::vector<blob> read_point(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds) {
    
    std::vector<pt> points;
    dedelta x,y;
    size_t pos=0;
    while (pos<cmds.size()) {
        if ((cmds[pos]&7)!=1) { throw std::domain_error("not a point: first cmd not a moveto"); }
        size_t npts = cmds[pos] >> 3;    
            
        pos++;
        points.reserve(points.size()+npts);
        for (size_t i=0; i < npts; i++) {
            points.push_back(forward(x(cmds[pos]), y(cmds[pos+1]),np));
            pos+=2;
        }
    }
    
    std::vector<blob> result;
    result.reserve(points.size());
    for (const auto& p: points) {
        std::string s(21, '\0');
        s[0] = '\1';
        s[1] = '\1';
        oqt::write_double_le(s, 5, p.first);
        oqt::write_double_le(s, 13, p.second);
        result.push_back(blob(s));
    }
    
    return result;
}

double ring_area(const std::vector<std::pair<double,double>>& r) {
    if (r.size()<3) { throw std::domain_error("not a ring"); }
    
    
    double a=0;
    for (size_t i=0; i < (r.size()-1); i++) {
        a+=(r[i+1].first-r[i].first)*(r[i+1].second+r[i].second);
    }
    return a/2;
}




size_t read_ring_old(std::vector<std::vector<std::pair<double,double>>>& rings, const transform_func& forward, int64_t np, dedelta& x, dedelta& y,const std::vector<oqt::uint64>& cmds, size_t pos, bool ispoly) {
    
    if ((cmds[pos])!=9) { throw std::domain_error("failed: ring doesn't start with a single moveto"); }
    
    
    if ((cmds[pos+3]&7) != 2) { throw std::domain_error("failed: ring doesn't have a lineto after the first moveto"); }
    size_t nlt = cmds[pos+3]>>3;
    size_t endpos = pos + 4 + 2*nlt;
    size_t npts = nlt + 1;
    
    if (ispoly) {
        if ((cmds[pos+4 + 2*nlt])!= 15) { throw std::domain_error("failed: polygon ring doesn't end with a single close"); }
        npts += 1;
        endpos+=1;
    }
    
    std::vector<std::pair<double,double>> ring;
    ring.reserve(npts+(ispoly?1:0));
    
    ring.push_back(forward(x(cmds[pos+1]), y(cmds[pos+2]),np));
    for (size_t j = 0; j < nlt; j++) {
        ring.push_back(forward(x(cmds[pos+4+2*j]), y(cmds[pos+5+2*j]),np));
       
    }
    
    if (ispoly) {
        ring.push_back(ring[0]);
        
    }
    
        
    rings.push_back(ring);
    return endpos;
}



std::vector<std::vector<std::pair<double,double>>> read_rings_old(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds, bool is_poly) {
    size_t pos=0;
    
    std::vector<std::vector<std::pair<double,double>>> result;
    dedelta x, y;
    while (pos < cmds.size()) {
        pos = read_ring_old(result, forward, np, x, y, cmds, pos,is_poly);
    }
    return result;
}


size_t read_next_cmd(std::vector<std::vector<std::pair<double,double>>>& result, const transform_func& forward, dedelta& x, dedelta& y, int64_t np, const std::vector<oqt::uint64>& cmds, size_t pos) {
    
    size_t c = cmds[pos] & 7;
    size_t l = cmds[pos] >> 3;
    
    pos++;
    if (c==1) {
        for (size_t i=0; i < l; i++) {
            double xx = x(cmds[pos]);
            double yy = y(cmds[pos+1]);
            result.push_back({forward(xx,yy,np)});            
            pos+=2;
        }
    } else if (c==2) {
        if (result.empty()) { throw std::domain_error("lineto without moveto?"); }
        result.back().reserve(1+l);
        for (size_t i=0; i < l; i++) {
            double xx = x(cmds[pos]);
            double yy = y(cmds[pos+1]);
            result.back().push_back(forward(xx,yy,np));            
            pos+=2;
        }
    } else if (c==7) {
        if (l!=1) { throw std::domain_error("close with len="+std::to_string(l)); }
        if (result.empty()) { throw std::domain_error("close without moveto?"); }
        result.back().push_back(result.back().front());
    }
    return pos;
}
    

std::vector<std::vector<std::pair<double,double>>> read_rings(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds, size_t gt) {
    size_t pos=0;
    
    std::vector<std::vector<std::pair<double,double>>> result;
    dedelta x, y;
    while (pos < cmds.size()) {
        pos = read_next_cmd(result, forward, x, y, np, cmds, pos);
    }
    
    for (const auto& r: result) {
        if (gt==1) {
            if (r.size()!=1) { throw std::domain_error("point with "+std::to_string(r.size())+" points?"); }
        } else if (gt==2) {
            if (r.size()<2) { throw std::domain_error("line with "+std::to_string(r.size())+" points?"); }
        }  else if (gt==3) {
            if (r.size()<3) {
                throw std::domain_error("ring with "+std::to_string(r.size())+" points?");
            } else if (r.front()!=r.back()) {
                throw std::domain_error("ring with not a ring?");
            }
        }
    }
    return result;
}
        



std::string pack_linestring(const std::vector<std::pair<double,double>>& line) {
    std::string result(1+8+16*line.size(), '\0');
    result[0]='\1';
    result[1]='\2';
    
    size_t pos=5;
    pos = oqt::write_uint32_le(result, pos, line.size());
    for (const auto& p: line) {
        pos = oqt::write_double_le(result, pos, p.first);
        pos = oqt::write_double_le(result, pos, p.second);
    }
    
    return result;
}
        

std::vector<blob> read_linestring(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds) {
        
    auto rings = read_rings(forward, np, cmds, 2);
    
    std::vector<blob> result;
    result.reserve(rings.size());
    for (const auto& r: rings) {
        result.push_back(blob(pack_linestring(r)));
    }
    
    return result;
}

std::string pack_polygon(const std::vector<std::vector<std::pair<double,double>>>& rings) {
    
    size_t tl = 1 + 8;
    for (const auto& ring: rings) {
        tl+=4;
        tl+=16*ring.size();
    }
    
    std::string result(tl,'\0');
    result[0] = '\1';
    result[1] = '\3';
    size_t pos=5;
    pos=oqt::write_uint32_le(result, pos, rings.size());
    
    for (const auto& ring: rings) { 
    
        pos=oqt::write_uint32_le(result, pos, ring.size());
        for (const auto& p: ring) {
            pos=oqt::write_double_le(result, pos, p.first);
            pos=oqt::write_double_le(result, pos, p.second);
        }
    }
    
    return result;
}
   
std::vector<blob> read_polygon(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds) {
    auto rings = read_rings(forward,np,cmds,3);
    
    if (rings.empty()) { return {}; }
    std::vector<blob> out;
    
    std::vector<std::vector<std::pair<double,double>>> poly_rings;
    for (const auto& ring: rings) {
        
        double a = ring_area(ring);
        if (a>0) {
            if (!poly_rings.empty()) {
                out.push_back(blob(pack_polygon(poly_rings)));
            }
            poly_rings.clear();
        }
        poly_rings.push_back(ring);
    }
    
    if (!poly_rings.empty()) {
        out.push_back(blob(pack_polygon(poly_rings)));
    }
    
    return out;
}  

/*
size_t read_ring(std::vector<std::string>& rings, const transform_func& forward, int64_t np, dedelta& x, dedelta& y,const std::vector<oqt::uint64>& cmds, size_t pos, bool ispoly) {
    
    if ((cmds[pos])!=9) { throw std::domain_error("failed: ring doesn't start with a single moveto"); }
    
    
    if ((cmds[pos+3]&7) != 2) { throw std::domain_error("failed: ring doesn't have a lineto after the first moveto"); }
    size_t nlt = cmds[pos+3]>>3;
    size_t endpos = pos + 4 + 2*nlt;
    size_t npts = nlt + 1;
    
    if (ispoly) {
        if ((cmds[pos+4 + 2*nlt])!= 15) { throw std::domain_error("failed: polygon ring doesn't end with a single close"); }
        npts += 1;
        endpos+=1;
    }
    
    std::string result(4 + npts*16, '\0');
    
    size_t wpos = oqt::write_uint32_le(result, 0, npts);
    
       
    wpos = add_point(result, wpos, forward, np, x(cmds[pos+1]), y(cmds[pos+2]));
    for (size_t j = 0; j < nlt; j++) {
        wpos = add_point(result, wpos, forward, np, x(cmds[pos+4+2*j]), y(cmds[pos+5+2*j]));
    }
    
    if (ispoly) {
        std::copy(result.begin()+4, result.begin()+20, result.begin()+wpos);
        wpos+=16;
    }
    
    if (wpos !=result.size()) { throw std::domain_error("failed: ring length wrong?"); }
    
    rings.push_back(result);
    return endpos;
}

std::vector<std::string> read_rings(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds, bool is_poly) {
    size_t pos=0;
    
    std::vector<std::string> result;
    dedelta x, y;
    while (pos < cmds.size()) {
        pos = read_ring(result, forward, np, x, y, cmds, pos,is_poly);
    }
    return result;
}


size_t read_ring_area(std::vector<std::pair<double,std::string>>& rings, const transform_func& forward, int64_t np, dedelta& x, dedelta& y,const std::vector<oqt::uint64>& cmds, size_t pos, bool ispoly) {
    
    if ((cmds[pos])!=9) { throw std::domain_error("failed: ring doesn't start with a single moveto"); }
    
    
    if ((cmds[pos+3]&7) != 2) { throw std::domain_error("failed: ring doesn't have a lineto after the first moveto"); }
    size_t nlt = cmds[pos+3]>>3;
    size_t endpos = pos + 4 + 2*nlt;
    size_t npts = nlt + 1;
    
    if (ispoly) {
        if ((cmds[pos+4 + 2*nlt])!= 15) { throw std::domain_error("failed: polygon ring doesn't end with a single close"); }
        npts += 1;
        endpos+=1;
    }
    
    std::string result(4 + npts*16, '\0');
    
    size_t wpos = oqt::write_uint32_le(result, 0, npts);
    
    
    int64_t ox=x(cmds[pos+1]);
    int64_t oy=y(cmds[pos+2]);
    
    int64_t px=ox; int64_t py=oy;
    double a=0;
    
    wpos = add_point(result, wpos, forward, np, px, py);
    for (size_t j = 0; j < nlt; j++) {
        int64_t nx=x(cmds[pos+4+2*j]);
        int64_t ny=y(cmds[pos+5+2*j]);
        wpos = add_point(result, wpos, forward, np, nx, ny);
        a += (nx-px)*(ny+py);
        px=nx; py=ny;
    }
    
    if (ispoly) {
        
        a += (ox-px)*(oy+py);
        
        std::copy(result.begin()+4, result.begin()+20, result.begin()+wpos);
        wpos+=16;
    }
    
    if (wpos !=result.size()) { throw std::domain_error("failed: ring length wrong?"); }
    
    rings.push_back(std::make_pair(a/2.0, result));
    return endpos;
}


std::vector<std::pair<double,std::string>> read_rings_area(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds, bool is_poly) {
    size_t pos=0;
    
    std::vector<std::pair<double,std::string>> result;
    dedelta x, y;
    while (pos < cmds.size()) {
        pos = read_ring_area(result, forward, np, x, y, cmds, pos,is_poly);
    }
    return result;
}


std::vector<blob> read_linestring(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds) {
    
    std::vector<std::string> rings = read_rings(forward,np,cmds,false);
    
    std::vector<blob> result;
    for (const auto s: rings) {
        result.push_back(blob("\1\2\0\0\0"s + s));
    }
    return result;
    
}   

size_t add_ring(std::vector<blob>& out, const std::vector<std::pair<double,std::string>>& rings, size_t p) {
    
    auto q = [&rings,p]() {
    
        size_t qq=p+1;
        if (qq<rings.size()) {
            for ( ; qq<rings.size(); qq++) {
                if (rings[qq].first > 0) {
                    return qq;
                }
            }
        }
        return qq;
    }();
    
    
    size_t tl = 9;
    for (size_t i=p; i < q; i++) {
        tl+=rings[i].second.size();
    }
    
    std::string result(tl,'\0');
    result[0]='\1';
    result[1]='\3';
    size_t pos = oqt::write_uint32_le(result, 5, rings.size());
    
    for (size_t i=p; i < q; i++) {
        
        std::copy(rings[i].second.begin(),rings[i].second.end(),result.begin()+pos);
        pos+=rings[i].second.size();
    }
    
    out.push_back(blob(result));
    return q;
}
    

std::vector<blob> read_polygon(const transform_func& forward, int64_t np, const std::vector<oqt::uint64>& cmds) {
    
    std::vector<std::pair<double,std::string>> rings = read_rings_area(forward,np,cmds,true);
    //if (rings.size()==0) { throw std::domain_error("no polygons"); }
    if (rings.size()==0) { return {}; }
    
    std::vector<blob> out;
    
    
    size_t p=0;
    while (p < rings.size()) {
        p = add_ring(out, rings, p);
    }
    
    return out;
}  
*/

std::vector<blob> read_mvt_geometry(const transform_func& forward, int64_t np, oqt::uint64 gt, const std::vector<oqt::uint64>& cmds) {
    if (gt==1) {
        return read_point(forward, np, cmds);
    } else if (gt==2) {
        return read_linestring(forward, np, cmds);
    } else if (gt==3) {
        return read_polygon(forward, np, cmds);
    }
    throw std::domain_error("unexpected geometry type "+std::to_string(gt));
    return {};
}


mvt_feature read_mvt_feature(
    const transform_func& forward, int64_t np,
    const std::vector<std::string>& keys, const std::vector<std::string>& vals, 
    const std::string& data, bool pass_geoms, std::string tile_key) {
    
    
    mvt_feature feat;
    
    feat.id=0;
    feat.minzoom=-1;
    size_t gt=0;
    //std::vector<oqt::uint64> cmds;
    std::string cmds;
    
    
    size_t pos=0;
    oqt::PbfTag tg = oqt::read_pbf_tag(data, pos);
    for ( ; tg.tag>0; tg=oqt::read_pbf_tag(data, pos)) {
        if (tg.tag==1) {
            feat.id = (int64_t) tg.value;
        } else if (tg.tag==2) {
            auto ii = oqt::read_packed_int(tg.data);
            for (size_t i=0; i < ii.size(); i+=2) {
                const std::string& k = keys.at(ii.at(i));
                const std::string& v = vals.at(ii.at(i+1));
                
                if (k=="min_zoom") {
                    try {
                        feat.minzoom = read_value_integer(v);
                    } catch ( ... ) {}
                } else {
                    feat.properties.insert(std::make_pair(k,blob{v}));
                }
            }
        } else if (tg.tag==3) {
            gt=tg.value;
        } else if (tg.tag==4) {
            cmds = tg.data;
        } else if (tg.tag==8) {
            //bounds, skip
        }
    }
    if (pass_geoms) {
        feat.geometries.push_back(oqt::pack_pbf_tags({
                oqt::PbfTag{3,gt,""},
                oqt::PbfTag{4,0,cmds},
                oqt::PbfTag{5,(oqt::uint64) np,""},
                oqt::PbfTag{6,0,tile_key}
            }));
    } else {
        try {
            feat.geometries=read_mvt_geometry(forward, np, gt, oqt::read_packed_int(cmds));
        } catch (...) {}
    }
    return feat;
}       




void read_mvt_layer(data_map& result, const transform_func& forward, const std::string& data, bool pass_geoms, std::string tile_key) {
    
    
    std::vector<std::string> feature_blobs;
    std::vector<std::string> keys;
    std::vector<std::string> vals;
    std::string table;
    int64_t np=4096;
    
    size_t pos=0;
    oqt::PbfTag tg = oqt::read_pbf_tag(data, pos);
    for ( ; tg.tag>0; tg=oqt::read_pbf_tag(data, pos)) {
        if (tg.tag==15) {
            if (tg.value!=2) { throw std::domain_error("wrong version??"); }
        } else if (tg.tag==1) {
          table=tg.data;  
        } else if (tg.tag==2) {
            feature_blobs.push_back(tg.data);
        } else if (tg.tag==3) {
            keys.push_back(tg.data);
        } else if (tg.tag==4) {
            vals.push_back(tg.data);
        } else if (tg.tag==5) {
            np = tg.value;
        } else if (tg.tag==9) {
            np = tg.value; //TEMPORARY: IDIOT CODING IN OQTTILES
        }
    }
    //std::cout << "layer " << table << " (np=" << np << ") " << keys.size() << " keys, "  << " " << vals.size() << " vals, " << " " << feature_blobs.size() << " features"  << std::endl;
    
    auto& features = result[table];
    features.reserve(features.size()+feature_blobs.size());
    for (const auto& f: feature_blobs) {
        auto feat = read_mvt_feature(forward, np, keys, vals, f, pass_geoms, tile_key);
        if (feat.geometries.empty()) {
            //pass
        } else {
            features.push_back(feat);
        }
    }
    
    
}

  
transform_func make_transform(int64_t tx, int64_t ty, int64_t tz) {
    const double earth_width = 40075016.68557849;

    double ts = earth_width / (1ll<<tz);
    double x0 = tx*ts - earth_width/2;
    double y0 = earth_width/2 - (ty)*ts;
    
    return [ts,x0,y0](double x, double y, double np) { return std::make_pair(x0 + (ts*x)/np, y0 - (ts*y)/np); };
}




std::vector<blob> read_mvt_geometry_call(int64_t tx, int64_t ty, int64_t tz, int64_t np, int64_t gt, std::string cmds_in) {
    auto cmds = oqt::read_packed_int(cmds_in);
    
    transform_func forward = make_transform(tx,ty,tz);
    
    return read_mvt_geometry(forward, np, gt, cmds);
}

std::vector<blob> read_mvt_geometry_packed(std::string data) {
    int64_t np=0;
    int64_t gt=0;
    std::vector<oqt::uint64> cmds;
    std::vector<oqt::uint64> tile_key;
    
    size_t pos=0;
    auto tg = oqt::read_pbf_tag(data,pos);
    for ( ; tg.tag>0; tg=oqt::read_pbf_tag(data,pos)) {
        if (tg.tag==3) { gt = tg.value; }
        if (tg.tag==4) { cmds=oqt::read_packed_int(tg.data); }
        if (tg.tag==5) { np = tg.value; }
        if (tg.tag==6) { tile_key=oqt::read_packed_int(tg.data); }
    }
    
    if (tile_key.size()==3) {
        transform_func forward = make_transform(tile_key[0],tile_key[1],tile_key[2]);
        return read_mvt_geometry(forward, np, gt, cmds);
    } else {
        transform_func forward = [](double x, double y, double npi) { return std::make_pair(x*256./npi, y*256./npi); }; 
        return read_mvt_geometry(forward, np, gt, cmds);
    }

}

  

data_map read_mvt_tile(const std::string& data_in, int64_t tx, int64_t ty, int64_t tz, bool gzipped, bool pass_geoms) {
    
    if (gzipped) {
        return read_mvt_tile(oqt::decompress_gzip(data_in), tx, ty, tz, false, pass_geoms);
    }
    
    transform_func forward = make_transform(tx,ty,tz);
    std::string tile_key = oqt::write_packed_int({(oqt::uint64) tx, (oqt::uint64) ty, (oqt::uint64) tz});
    
    data_map result;
    
    size_t pos=0;
    oqt::PbfTag tg = oqt::read_pbf_tag(data_in, pos);
    
    for ( ; tg.tag>0; tg=oqt::read_pbf_tag(data_in, pos)) {
        if (tg.tag==3) {
            read_mvt_layer(result, forward, tg.data, pass_geoms, tile_key);
        } else {
            std::cout << "?? " << tg.tag << " " << tg.value << " " << tg.data << std::endl;
        }
    }
    
    return result;
}







