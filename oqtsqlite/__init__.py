#-----------------------------------------------------------------------
#
# This file is part of oqtsqlite
#
# Copyright (C) 2018 James Harris
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
#-----------------------------------------------------------------------

from __future__ import print_function

import math, os, json, time
import pymapnik11 as mk
from .utils import time_op, print_sameline
import oqt
from . import sqlparse, _oqtsqlite, mbtiles
#import sqlselect
#import oqttiles as ot

def query_to_sqlite(qu):
    pp = sqlparse.proc(qu)
    return pp.sqlite


#_oqtsqlite.BindElement2.insert_element = _oqtsqlite.insert_element
#_oqtsqlite.BindElement2.insert_python = _oqtsqlite.insert_python
default_node_cols = ['access', 'addr:housename', 'addr:housenumber', 'addr:interpolation', 'admin_level', 'aerialway', 'aeroway', 'amenity', 'barrier', 'bicycle', 'boundary', 'bridge', 'building', 'construction', 'covered', 'embankment', 'foot', 'highway', 'historic', 'horse', 'junction', 'landuse', 'leisure', 'lock', 'man_made', 'military', 'name', 'natural', 'oneway', 'place', 'power', 'railway', 'ref', 'religion', 'route', 'service', 'shop', 'surface', 'tourism', 'tunnel', 'water', 'waterway']
default_way_cols = ['access', 'addr:housename', 'addr:housenumber', 'addr:interpolation', 'admin_level', 'aerialway', 'aeroway', 'amenity', 'barrier', 'bicycle', 'boundary', 'bridge', 'building', 'construction', 'covered', 'embankment', 'foot', 'highway', 'historic', 'horse', 'junction', 'landuse', 'leisure', 'lock', 'man_made', 'military', 'name', 'natural', 'oneway', 'place', 'power', 'railway', 'ref', 'religion', 'route', 'service', 'shop', 'surface', 'tourism', 'tracktype', 'tunnel', 'water', 'waterway']


def prep_table_point(node_cols=default_node_cols, all_other_tags=True, tabname='planet_osm_point',filtercols=None):
    
    cols = [('osm_id','integer'), ('tile','integer'), ('quadtree','integer')]
    cols += [(n,'text') for n in node_cols if (filtercols is None or n in filtercols)]
    
    if all_other_tags:
        cols += [('tags','blob'),]
    
    cols+=[('minzoom','integer')]
    cols += [('layer', 'integer'), ('way','blob')]
    return _oqtsqlite.BindElement2([a for a,b in cols], tabname)

def prep_table_line(way_cols=default_node_cols, all_other_tags=True, tabname='planet_osm_line',filtercols=None):
    cols = [('osm_id','integer'), ('tile','integer'), ('quadtree','integer')]
    cols += [(n,'text') for n in default_way_cols if (filtercols is None or n in filtercols)]
    if all_other_tags:
        cols += [('tags','blob'),]
    
    cols+=[('minzoom','integer')]
    cols += [('layer', 'integer'), ('z_order','integer'),('length','float'),('way','blob')]
    return _oqtsqlite.BindElement2([a for a,b in cols], tabname)

def prep_table_polygon(way_cols=default_node_cols, all_other_tags=True,tabname='planet_osm_polygon', filtercols=None):
    cols = [('osm_id','integer'), ('part','integer'),('tile','integer'), ('quadtree','integer')]
    cols += [(n,'text') for n in default_way_cols if (filtercols is None or n in filtercols)]
    
    if all_other_tags:
        cols += [('tags','blob'),]
    
    cols+=[('minzoom','integer')]
    cols += [('layer', 'integer'), ('z_order','integer'),('way_area','float'),('way','blob')]
    
    return _oqtsqlite.BindElement2([a for a,b in cols], tabname)
    
default_views = [
    "create view planet_osm_highway as select * from planet_osm_line where z_order is not null and z_order!=0",
    "create view planet_osm_boundary as select osm_id,part,tile,quadtree,name,ref,admin_level,boundary,minzoom, way_area,way from planet_osm_polygon where osm_id<0 and boundary='administrative'",
    "create view planet_osm_building as select * from planet_osm_polygon where building is not null and building != 'no'",
]





class TilesFilter:
    def __init__(self, tiles,bounds):
        
        self.bounds=bounds
        self.cnt=0
        
        self.tiles={}
        for t in tiles:
            if not t.Quadtree in self.tiles:
                self.tiles[t.Quadtree]=[[],None]
            self.tiles[t.Quadtree][0].append(t)

    def __call__(self, bx,zoom):      
        for k,v in sorted(self.tiles.items()):
            if (k&31) <= zoom and bx.overlaps_quadtree(k):
                v[1]=self.cnt
                for t in v[0]:
                    yield t
                
        self.cnt+=1
        
    


class TilesCacheFile(TilesFilter):
    def __init__(self, geomsfn, max_tiles=200):
        super(TilesCacheFile).__init__(self, [], None)
        
        self.geomsfn = geomsfn
        self.header = oqt.pbfformat.get_header_block(geomsfn)
        
        self.max_tiles=max_tiles
        self.tiles={}
        self.bounds=self.header.Box
        
    def __call__(self, bx,zoom):
        if not self.header is None:
            self.update_tiles(bx, zoom)
        
        return TilesCacheFile.__call__(bx, zoom)
        
    def update_tiles(self, bx, zoom):
        sys.stdout.write("update_tiles %.55s %2d" % (repr(bx),zoom))
        sys.stdout.flush()
        st=time.time()
        ts,locs=set([]),[]
        for a,b,c in self.header.Index:
            if (a&31) <= zoom and bx.overlaps_quadtree(a):
                ts.add(a)
                if not a in self.tiles:
                    locs.append(b)
        
        sys.stdout.write("%d tiles required, %d already loaded" % (len(ts),len(self.tiles)))
        
        if len(locs)+len(self.tiles)>self.max_tiles:
            torm=len(locs)+len(self.tiles)-self.max_tiles
            rm=[(c,a) for a,(b,c) in self.tiles.items() if not a in ts]
            rm.sort()
            
            rm=rm[:torm]
            sys.stdout.write('remove  %d tiles' % (len(rm),))
            
            for _,k in rm:
                del self.tiles[k]
        
        if locs:
            sys.stdout.write('load %d tiles' % (len(locs),))
            
            nb=[]
            oqt.geometry.read_blocks_geometry(self.geomsfn, oqt.utils.addto(nb), locs)
            for t in nb:
                self.tiles[t.Quadtree]=[[t],None]
        sys.stdout.write(': %0.1fs\n' % (time.time()-st))
    
    
class TilesSlim:
    def __init__(self, geomsfn):
        self.geomsfn = geomsfn
        self.header = oqt.pbfformat.get_header_block(geomsfn)
        
        self.bounds=self.header.Box
            
    def __call__(self, bx, zoom):
        print_sameline('TilesSlim %.55s %2d' % (repr(bx),zoom))
        st=time.time()
        locs=[b for a,b,c in self.header.Index if (a&31) <= zoom and bx.overlaps(oqt.elements.quadtree_bbox(a,0))]
        print_sameline('load %d tiles,' % (len(locs),))
        
        ans=[]
        oqt.geometry.read_blocks_geometry(self.geomsfn, oqt.utils.addto(ans), locs, numchan=4, bbox=bx, minzoom=zoom)
        nobjs = sum(len(bl) for bl in ans)
        print(': %d objs, %0.1fs' % (nobjs, time.time()-st))
        return ans
    
class SqliteTilesBase(object):
    def __init__(self, bounds, style=None, extended=False, cols=None, views=None,shapetables={}):
        
        if not cols is None:
            self.cols=cols
            self.views=[]
            if not views is None:
                self.views=views
        else:
        
            
            style = style or {}
            self.cols = [
                prep_table_point(**style),
                prep_table_line(**style),
                prep_table_polygon(**style)
            ]
                
            self.views=default_views
            if extended:
                self.cols += [
                    prep_table_line(style, True, 'planet_osm_highway'),
                    prep_table_line(style, True, 'planet_osm_polygon_exterior'),
                    prep_table_polygon(style, True, 'planet_osm_building'),
                    prep_table_polygon(style, False, 'planet_osm_boundary',set(['name','admin_level','boundary','ref','way_area'])),
                    prep_table_polygon(style, True, 'planet_osm_polygon_point')
                ]
                self.views=[]
        
        if shapetables:
            for k,v in shapetables.items():
                self.cols.append(_oqtsqlite.BindElement2(v, 'planet_osm_'+k))
        
        self.bounds=bounds
        self.prev=None
        self.tms=[]
        
    def prep_sqlitestore(bx, zoom):
        raise Exception("not implemented")
        
    def __call__(self, box, zoom):
        if self.prev is not None:
            if [list(box), zoom] == self.prev[:2]:
                return self.prev[2]
        st=time.time()
        data,cc = self.prep_sqlitestore(box, zoom)
        self.tms.append((box, zoom, time.time()-st,cc))
        self.prev = [list(box),zoom,data]
        return data
        
        
    
class SqliteTiles(SqliteTilesBase):
    def __init__(self, tiles, bounds=None,style=None, extended=False,cols=None, views=None, shapetables={}):
        super(SqliteTiles,self).__init__(bounds,style,extended,cols, views,shapetables)
        self.tiles=tiles
        self.prev=None
        if bounds is None:
            if self.tiles.bounds.minx < -850000000 or self.tiles.bounds.maxx > 850000000:
                self.bounds=mk.tile_bound(0,0,0)
            else:
                self.bounds=mk.forward(mk.box2d(*[b*0.0000001 for b in self.tiles.bounds]))
        else:
            self.bounds=bounds
        
    def prep_sqlitestore(self, bx, zoom):
        data=SqliteStore(self.cols,zoom, views=self.views)
        
        fb=oqt.utils.bbox(*map(oqt.intm, mk.backward(bx)))
        
        cc=data.add_all(self.tiles(fb,zoom), fb, zoom)
        
        return data,cc

            
    
class SqliteStore:
    def __init__(self, cols=None, zoom=None,fn=None,views=None,table_prfx=None):
        
        
        if cols is None:
            style=oqt.geometry.style.GeometryStyle()
            self.cols = [
                prep_table_point(style),
                prep_table_line(style),
                prep_table_polygon(style)
            ]
            
        else:
            self.cols=cols
        
        if table_prfx is None:
            self.table_prfx='planet_osm_'
        else:
            self.table_prfx=table_prfx
        
        self.views = default_views if views is None else views
        
        self.get_tables = _oqtsqlite.extended_get_tables if len(self.cols)>3 else _oqtsqlite.default_get_tables            
        
        exists = fn and os.path.exists(fn)
        
        self.conn = _oqtsqlite.SqliteDb(":memory:" if fn is None else fn)
        
        self.pixel_area='1.0'
        self.pixel_size='1.0'
        self.scale_denominator='3500.0'
        if zoom is not None:
            self.pixel_area='%0.1f' % (mk.zoom(zoom)**2,)
            self.pixel_size='%0.1f' % (mk.zoom(zoom),)
            self.scale_denominator='%0.1f' % (mk.zoom(zoom)/0.00028,)
        
            
        if not exists:            
            self.conn.execute("begin")
            for c in self.cols:
                self.conn.execute(c.table_create)
            
            
            for v in self.views:
                self.conn.execute(v)
            self.conn.execute("commit")    
    def add_all(self, tiles, filter_box=None, minzoom=None):
        nums=[0 for o in self.cols]
        self.conn.execute("begin")
        for tile in tiles:
            if not minzoom is None:
                if tile.Quadtree&31 > minzoom:
                    continue
        
            if not filter_box is None:
                if not filter_box.overlaps_quadtree(tile.Quadtree):
                    continue
        
        
            nn= self.add_tile(tile, filter_box, minzoom)
            for i,n in enumerate(nn):
                nums[i]+=n
        self.conn.execute("commit")
        return nums
        
    def add_tile_xx(self, tile, filter_box=None, minzoom=None):
        cc=[0 for c in self.cols]
        for obj in tile:
            if minzoom is None or obj.MinZoom <= minzoom:
                if filter_box is None or filter_box.overlaps(obj.Bounds):
                    tt=self.get_tables(obj)
                    for i,col in enumerate(self.cols):
                        if (tt & (1<<i)):
                            cc[i]+=_oqtsqlite.insert_element(col, self.conn, obj, tile.Quadtree)
                            
        return cc
        
    def add_tile(self, tile, filter_box=None, minzoom=None):    
        return _oqtsqlite.insert_block(
            self.conn,
            self.cols,
            tile,
            oqt.utils.bbox(18000000000,900000000,-1800000000,-900000000) if filter_box is None else filter_box,
            -1 if minzoom is None else minzoom,
            self.get_tables
            )
    
    
    def get_col(self, tab):
        if tab=='polypoint':
            tab='polygon_point'
        for c in self.cols:
            if c.table_name==self.table_prfx+tab:
                return c
        
        if tab in ('building','boundary'):
            return self.cols[2]
                
        elif tab=='highway':
            return self.cols[1] 
        return None
    
    def add_mvt(self, tx, ty, tz, data, minzoom=None,merge_geoms=False,pass_geoms=False):
        
        return _oqtsqlite.insert_mvt_tile(self.conn, lambda c: self.get_col(c), data, tx, ty, tz, True, minzoom,merge_geoms,pass_geoms)
        
        
    def add_mvt_old(self, tx, ty, tz, data, minzoom=None):
        feats=_oqtsqlite.read_mvt_tile(data,tx,ty,tz,True)
        
        
        
        #print 'add_mvt', tx, ty, tz, [(k,len(v)) for k,v in feats.items()] 
        
        dd=set()
        
        split_multis=False#True
        cc=0  
        for tab,v in feats.items():
            
            bind_ele=self.get_col(tab)
            
            if not bind_ele:
                #print "skip table", tab, tx,ty,tz,tab
                continue
            
            
            tile_qt=oqt.elements.quadtree_from_tuple(tx,ty,tz)
            
                     
            for feat in v:
                if (feat.minzoom>=0) and (minzoom is None or feat.minzoom<=minzoom):
                    
                                            
                    pp = {'osm_id': feat.id, 'minzoom': feat.minzoom, 'tile':tile_qt}
                    
                    for k,v in feat.properties.items():
                        if not v: continue
                        
                        if k=='way_length': k='length'
                        if k=='min_zoom': continue
                        
                        if k in ('length','way_area'):
                            pp[k]=_oqtsqlite.read_value_double(v)
                        elif k in ('quadtree','z_order','layer','part'):
                            pp[k]=_oqtsqlite.read_value_integer(v)
                        
                        else:
                            pp[k]=_oqtsqlite.read_value_string(v)
                    
                    for g in feat.geometries:        
                        pp['way']=g
                        cc+=_oqtsqlite.insert_python(bind_ele,self.conn, pp)
                    
                    
        return cc
    
    def __call__(self, qu, ctx=None):
        
        qu_fix = qu.replace(':pixel_area:', self.pixel_area)
        qu_fix = qu_fix.replace(':pixel_size:', self.pixel_size)
        qu_fix = qu_fix.replace(':scale_denominator:', self.scale_denominator)
        
        
        if ctx is None:
            return self.conn.execute(qu_fix)
        
        return self.conn.execute_featureset(qu_fix,None if ctx is True else ctx)


class SqliteTilesMvt(SqliteTilesBase):
    def __init__(self, tiles, bounds, style=None, extended=False, cols=None,views=None,table_prfx=None,merge_geoms=False, pass_geoms=False, shapetables={}):
        super(SqliteTilesMvt,self).__init__(bounds,style,extended,cols,views, shapetables)
        
        self.table_prfx=table_prfx
        
        self.tiles=tiles
        
        self.minzoom=int(self.tiles.minzoom or 0)
        self.maxzoom=int(self.tiles.maxzoom or 14)
        self.merge_geoms=merge_geoms
        self.pass_geoms=pass_geoms
        
    def prep_sqlitestore(self, bx, zoom):
        
        tz=zoom
        if zoom>self.maxzoom: tz=self.maxzoom
        if zoom<self.minzoom: tz=self.minzoom
        
        minx, miny = mk.coord_to_tile(bx.minx, bx.maxy, tz)
        maxx, maxy = mk.coord_to_tile(bx.maxx, bx.miny, tz)
        
        st=time.time()
        print_sameline('%-50.50s zoom %2d: tile range: %8.2f,%8.2f,%2d => %8.2f, %8.2f,%2d: ' % (bx,zoom,minx,miny,tz,maxx,maxy,tz))
        data=SqliteStore(self.cols,zoom=zoom,views=self.views, table_prfx=self.table_prfx)
        cc,nt,tl=0,0,0
        for tx in range(int(minx),int(maxx)+1):
            for ty in range(int(miny),int(maxy)+1):
                dd=self.tiles(tx,ty,tz)
                if dd:
                    dds=bytes(dd)
                    nt+=1
                    tl+=len(dds)
                    cc+=data.add_mvt(tx,ty,tz,dds,zoom,self.merge_geoms,self.pass_geoms)
                    
        print('read %2d tiles [%6.1fkb], added %6d feats in %4.1fs' % (nt,tl/1024.0,cc,time.time()-st))
        return data, cc
        
        

    
class AltDs:
    def __init__(self, tiles, query, idx=None, msgs=False, split_multigeoms=False):
        self.envelope = tiles.bounds
        self.geom_type = mk.datasource_geometry_t.Unknown
        self.query = query
        self.tiles = tiles
        self.idx=idx
        self.msgs=msgs
        self.times=[]
        self.split_multigeoms=split_multigeoms
       
    
    def __call__(self, query):
        try:
            #minzoom = int(math.ceil(mk.utils.find_zoom(1/query.resolution[0])))
            minzoom = int(round(mk.utils.find_zoom(1/query.resolution[0])))
            a,tables = mk.utils.time_op(self.tiles, query.bbox, minzoom)
            
            ctx=mk.context()
            for pn in query.property_names: ctx.push(pn)
            
            b,result = mk.utils.time_op(tables, self.query, ctx)
                        
            self.times.append((query,minzoom,a,b))
            if self.msgs and self.idx is not None:
                print("layer %d: %0.3fs, %0.3fs, %0.3fs" % (self.idx, a, b, c))
            
            return result
            
        except Exception as ex:
            print(ex)
            return mk.make_python_featureset([])
    
    
    def set_to_layer(self, mp, i):
        mp.layers[i].datasource = mk.make_python_datasource({'type':'python', 'query': str(self.query)}, self.envelope, self.geom_type, self)
    



def convert_postgis_to_altds(mp, tiles, tabpp=None, convert_shapefiles=None):
    dses={}
    
    tm=[]
    
    for i,l in enumerate(mp.layers):
        if 'type' in l.datasource.params() and l.datasource.params()['type']=='postgis':
            try:
                tt=l.datasource.params()['table']
                if tabpp is not None:
                    tt=tt.replace(tabpp,'planet_osm')
                
                t,mm = time_op(query_to_sqlite,tt)
                tm.append((i,t))
                dses[i] = AltDs(tiles, mm, idx=i)
            except Exception as ex:
                print('layer %d failed: %s' % (i,ex))
        elif convert_shapefiles is not None and l.datasource.params()['type']=='shape':
            fn = l.datasource.params()['file']
            fna,fnb = os.path.split(fn)
            if fnb in convert_shapefiles:
                qq='select * from (select * from planet_osm_%s) as oo' % (convert_shapefiles[fnb],)
                t,mm=time_op(query_to_sqlite,qq)
                tm.append((i,t))
                dses[i]=AltDs(tiles, mm, idx=i)
            else:
                dses[i]=None
    for k,v in dses.items():
        if v:
            v.set_to_layer(mp, k)
        else:
            mp.layers[k].datasource=mk.make_python_datasource({'type':'python'}, tiles.bounds, mk.datasource_geometry_t.Unknown, lambda *a: mk.make_python_featureset([]))
    
    return dses
        
        

