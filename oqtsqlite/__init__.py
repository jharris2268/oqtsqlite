import math, os, json, time
import pymapnik11 as mk
import utils as ut
import oqt
from . import sqlparse, _oqtsqlite
#import sqlselect
#import oqttiles as ot

def query_to_sqlite(qu):
    pp = sqlparse.proc(qu)
    return pp.sqlite


#_oqtsqlite.BindElement2.insert_element = _oqtsqlite.insert_element
#_oqtsqlite.BindElement2.insert_python = _oqtsqlite.insert_python

def prep_table_point(style, tabname='planet_osm_point',filtercols=None):
    
    cols = [('osm_id','integer'), ('tile','integer'), ('quadtree','integer')]
    for k,v in sorted(style.keys.iteritems()):
        if filtercols is not None:
            if not k in filtercols:
                continue
        if k in ('XXX','layer'): continue

        if v.IsNode:
            cols.append((k,'text'))
    
    if style.other_tags:
        cols += [('tags','blob'),]
    
    cols+=[('minzoom','integer')]
    cols += [('layer', 'integer'), ('way','blob')]
    return _oqtsqlite.BindElement2([a for a,b in cols], tabname)

def prep_table_line(style, tabname='planet_osm_line',filtercols=None):
    cols = [('osm_id','integer'), ('tile','integer'), ('quadtree','integer')]
    for k,v in sorted(style.keys.iteritems()):
        if filtercols is not None:
            if not k in filtercols:
                continue
        if k in ('XXX','layer'): continue
        if v.IsWay:
            cols.append((k,'text'))
    
    if style.other_tags:
        cols += [('tags','blob'),]
    
    cols+=[('minzoom','integer')]
    cols += [('layer', 'integer'), ('z_order','integer'),('length','float'),('way','blob')]
    return _oqtsqlite.BindElement2([a for a,b in cols], tabname)

def prep_table_polygon(style,tabname='planet_osm_polygon', filtercols=None):
    cols = [('osm_id','integer'), ('part','integer'),('tile','integer'), ('quadtree','integer')]
    for k,v in sorted(style.keys.iteritems()):
        if filtercols is not None:
            if not k in filtercols:
                continue
        if k in ('XXX','layer'): continue
        if v.IsWay:
            cols.append((k,'text'))
    
    if style.other_tags:
        cols += [('tags','blob'),]
    
    cols+=[('minzoom','integer')]
    cols += [('layer', 'integer'), ('z_order','integer'),('way_area','float'),('way','blob')]
    
    return _oqtsqlite.BindElement2([a for a,b in cols], tabname)
    
default_views = [
    "create view planet_osm_highway as select * from planet_osm_line where z_order is not null and z_order!=0",
    "create view planet_osm_boundary as select osm_id,part,tile,quadtree,name,admin_level,boundary,minzoom, way from planet_osm_polygon where osm_id<0 and boundary='administrative'",
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
        for k,v in sorted(self.tiles.iteritems()):
            if (k&31) <= zoom and bx.overlaps_quadtree(k):
                v[1]=self.cnt
                for t in v[0]:
                    yield t
                
        self.cnt+=1
        
    


class TilesCacheFile(TilesFilter):
    def __init__(self, geomsfn, max_tiles=200):
        super(TilesCacheFile).__init__(self, [], None)
        
        self.geomsfn = geomsfn
        self.header = oqt._oqt.get_header_block(geomsfn)
        
        self.max_tiles=max_tiles
        self.tiles={}
        self.bounds=self.header.Box
        
    def __call__(self, bx,zoom):
        if not self.header is None:
            self.update_tiles(bx, zoom)
        
        return TilesCacheFile.__call__(bx, zoom)
        
    def update_tiles(self, bx, zoom):
        print 'update_tiles %.55s %2d' % (repr(bx),zoom),
        st=time.time()
        ts,locs=set([]),[]
        for a,b,c in self.header.Index:
            if (a&31) <= zoom and bx.overlaps_quadtree(a):
                ts.add(a)
                if not a in self.tiles:
                    locs.append(b)
        
        print '%d tiles required, %d already loaded' % (len(ts),len(self.tiles)),
        
        if len(locs)+len(self.tiles)>self.max_tiles:
            torm=len(locs)+len(self.tiles)-self.max_tiles
            rm=[(c,a) for a,(b,c) in self.tiles.iteritems() if not a in ts]
            rm.sort()
            
            rm=rm[:torm]
            print 'remove  %d tiles' % (len(rm),),
            
            for _,k in rm:
                del self.tiles[k]
        
        if locs:
            print 'load %d tiles' % (len(locs),),
            
            nb=[]
            oqt._oqt.read_blocks_geometry(self.geomsfn, oqt.utils.addto(nb), locs)
            for t in nb:
                self.tiles[t.Quadtree]=[[t],None]
        print ': %0.1fs' % (time.time()-st)
    
    

class TilesSlim:
    def __init__(self, geomsfn):
        self.geomsfn = geomsfn
        self.header = oqt._oqt.get_header_block(geomsfn)
        
        self.bounds=self.header.Box
            
    def __call__(self, bx, zoom):
        print 'TilesSlim %.55s %2d' % (repr(bx),zoom),
        st=time.time()
        locs=[b for a,b,c in self.header.Index if (a&31) <= zoom and bx.overlaps(oqt._oqt.quadtree_bbox(a,0))]
        print 'load %d tiles,' % (len(locs),),
        
        ans=[]
        oqt._oqt.read_blocks_geometry(self.geomsfn, oqt.utils.addto(ans), locs, numchan=4, bbox=bx, minzoom=zoom)
        nobjs = sum(len(bl) for bl in ans)
        print ': %d objs, %0.1fs' % (nobjs, time.time()-st)
        return ans
    
class SqliteTilesBase(object):
    def __init__(self, bounds, style=None, extended=False):
        
        if style is None:
            style=oqt.geometrystyle.GeometryStyle()
            
        self.cols = [
            prep_table_point(style),
            prep_table_line(style),
            prep_table_polygon(style)]
        self.views=default_views
        if extended:
            self.cols += [
                prep_table_line(style, 'planet_osm_highway'),
                prep_table_line(style, 'planet_osm_polygon_exterior'),
                prep_table_polygon(style, 'planet_osm_building'),
                prep_table_polygon(style, 'planet_osm_boundary',set(['name','admin_level','boundary'])),
                prep_table_polygon(style, 'planet_osm_polypoint')]
            self.views=[]
        
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
    def __init__(self, tiles, bounds=None,style=None, extended=False):
        super(SqliteTiles,self).__init__(bounds,style,extended)
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
        
        fb=oqt._oqt.bbox(*map(oqt.intm, mk.backward(bx)))
        
        cc=data.add_all(self.tiles(fb,zoom), fb, zoom)
        
        return data,cc

            
    
class SqliteStore:
    def __init__(self, cols=None, zoom=None,fn=None,views=None):
        
        
        if cols is None:
            style=oqt.geometrystyle.GeometryStyle()
            self.cols = [
                prep_table_point(style),
                prep_table_line(style),
                prep_table_polygon(style)
            ]
            
        else:
            self.cols=cols
        
        self.views = default_views if views is None else views
        
        self.get_tables = _oqtsqlite.extended_get_tables if len(self.cols)>3 else _oqtsqlite.default_get_tables            
        
        exists = fn and os.path.exists(fn)
        
        self.conn = _oqtsqlite.SqliteDb(":memory:" if fn is None else fn)
        
        self.pixel_area='1.0'
        if zoom is not None:
            self.pixel_area='%0.1f' % (mk.zoom(zoom)**2,)
        
            
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
            oqt._oqt.bbox(18000000000,900000000,-1800000000,-900000000) if filter_box is None else filter_box,
            -1 if minzoom is None else minzoom,
            self.get_tables
            )
    
    
    def get_col(self, tab):
        for c in self.cols:
            if c.table_name=='planet_osm_'+tab:
                return c
        
        if tab in ('building','boundary'):
            return self.cols[2]
                
        elif tab=='highway':
            return self.cols[1] 
        return None
    
    def add_mvt(self, tx, ty, tz, data, minzoom=None):
        
        return _oqtsqlite.insert_mvt_tile(self.conn, lambda c: self.get_col(c), data, tx, ty, tz, True, minzoom)
        
        
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
            
            
            tile_qt=oqt._oqt.quadtree_from_tuple(tx,ty,tz)
            
                     
            for feat in v:
                if (feat.minzoom>=0) and (minzoom is None or feat.minzoom<=minzoom):
                    
                                            
                    pp = {'osm_id': feat.id, 'minzoom': feat.minzoom, 'tile':tile_qt}
                    
                    for k,v in feat.properties.iteritems():
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
        
        
        if ctx is None:
            return self.conn.execute(qu_fix)
        
        return self.conn.execute_featureset(qu_fix,None if ctx is True else ctx)


class SqliteTilesMvt(SqliteTilesBase):
    def __init__(self, tiles, bounds, style=None, extended=False):
        super(SqliteTilesMvt,self).__init__(bounds)
        
        
            
        self.tiles=tiles
        
        self.minzoom=int(self.tiles.minzoom or 0)
        self.maxzoom=int(self.tiles.maxzoom or 14)
        
    def prep_sqlitestore(self, bx, zoom):
        
        tz=zoom
        if zoom>self.maxzoom: tz=self.maxzoom
        if zoom<self.minzoom: tz=self.minzoom
        
        minx, miny = mk.coord_to_tile(bx.minx, bx.maxy, tz)
        maxx, maxy = mk.coord_to_tile(bx.maxx, bx.miny, tz)
        
        st=time.time()
        print '%-50.50s zoom %2d: tile range: %8.2f,%8.2f,%2d => %8.2f, %8.2f,%2d: ' % (bx,zoom,minx,miny,tz,maxx,maxy,tz),
        data=SqliteStore(self.cols,zoom=zoom,views=self.views)
        cc,nt,tl=0,0,0
        for tx in xrange(int(minx),int(maxx)+1):
            for ty in xrange(int(miny),int(maxy)+1):
                dd=self.tiles(tx,ty,tz)
                if dd:
                    dds=str(dd)
                    nt+=1
                    tl+=len(dds)
                    cc+=data.add_mvt(tx,ty,tz,dds,zoom)
                    
        print 'read %2d tiles [%6.1fkb], added %6d feats in %4.1fs' % (nt,tl/1024.0,cc,time.time()-st)
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
    

def convert_postgis_to_altds(mp, tiles, tabpp=None):
    dses={}
    
    tm=[]
    
    for i,l in enumerate(mp.layers):
        if 'type' in l.datasource.params() and l.datasource.params()['type']=='postgis':
            try:
                tt=l.datasource.params()['table']
                if tabpp is not None:
                    tt=tt.replace(tabpp,'planet_osm')
                
                t,mm = ut.timeop(query_to_sqlite,tt)
                tm.append((i,t))
                dses[i] = AltDs(tiles, mm, idx=i)
            except Exception as ex:
                print('layer %d failed: %s' % (i,ex))
    
    for a,b in tm:
        if b>1.0:
            print "layer %d: %0.1fs" % (a,b)
    
    for k,v in dses.iteritems():
        v.set_to_layer(mp, k)
    
    return dses
        
        
