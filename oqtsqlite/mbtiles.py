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

import sqlite3, os, json


def prep_mbtiles(conn, name, minzoom, maxzoom, tiles_spec=None):
    curs=conn.cursor()
    curs.execute("create table metadata (name text, value text)")
    curs.execute("insert into metadata values ('name',?), ('type','baselayer'), ('version', '1'), ('description', 'country tiles'), ('format', 'pbf')", (name,))
    curs.execute("insert into metadata values ('minzoom',?), ('maxzoom',?)", (str(minzoom),str(maxzoom)))
    curs.execute("create table tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob)")
    curs.execute("create index tiles_tile on tiles (zoom_level,tile_column,tile_row)")
    if not tiles_spec is None:
        curs.execute("insert into metadata values ('tiles_spec', ?)", (json.dumps(tiles_spec),))
    
    
    curs.execute("create view tiles_alt as select tile_string(tile_column, tile_row, zoom_level) as tile, tile_column as x, tile_row as y, zoom_level as z, tile_data as data from tiles")
    conn.commit()

def prep_mbtiles_alt(conn, name, minzoom, maxzoom):
    curs=conn.cursor()
    curs.execute("create table metadata (name text, value text)")
    curs.execute("insert into metadata values ('name',?), ('type','baselayer'), ('version', '1'), ('description', 'country tiles'), ('format', 'pbf')", (name,))
    curs.execute("insert into metadata values ('minzoom',?), ('maxzoom',?)", (str(minzoom),str(maxzoom)))
    curs.execute("insert into metadata values ('schema','alt')")
    
    curs.execute("create table tile_data (basetile text, tile text, x integer, y integer, z integer, data blob)")
    curs.execute("create index tiles_xyz on tile_data (x, y, z)")
    curs.execute("create index tiles_tile on tile_data (tile)")
    curs.execute("create index tiles_basetile on tile_data (basetile)")
    
    curs.execute("create view tiles as select z as zoom_level, x as tile_column, y as tile_row, data as tile_data from tile_data")
    conn.commit()


class MBTiles(object):
    def __init__(self, fn, gettile=None, minzoom=0,maxzoom=16,create=False,newconn=False, tiles_spec=None, alt_schema=False, flipy=False):
        exists=os.path.exists(fn)
        if not exists and not create:
            raise IOError(2,'No such file: '+fn)
        
        self.fn=fn
        self.conn = sqlite3.connect(fn)
        self.conn.enable_load_extension(True)
        self.conn.execute("select load_extension('/home/james/work/tile_string.so')")
        
        if not exists:
            if alt_schema:
                prep_mbtiles_alt(self.conn, os.path.splitext(os.path.split(fn)[1])[0], minzoom, maxzoom)
            else:
                prep_mbtiles(self.conn, os.path.splitext(os.path.split(fn)[1])[0], minzoom, maxzoom, tiles_spec)
    
        self.curs = self.conn.cursor()
        
        
        self.tiles_spec = tiles_spec
        self.minzoom=minzoom
        self.maxzoom=maxzoom
        self.flipy = False
        self.alt_schema=False
        self.curs.execute("select * from metadata")
        self.metadata={}
        for a,b in self.curs:
            if a=='minzoom': self.minzoom=int(b)
            elif a=='maxzoom': self.maxzoom=int(b)
            elif a=='flipy': self.flipy = b=='true'
            elif a=='tiles_spec': self.tiles_spec=json.loads(b)
            elif a=='schema': self.alt_schema=b=='alt'
            else:
                self.metadata[a]=b
        
        if flipy is not None:
            self.flipy=flipy
        
        self.ii = 0
        
        self.gettile=gettile
        self.nt=0
        self.trans=False
        self.newconn=newconn
        
    def __del__(self):
        if self.trans:
            print('MBTiles __del__: rollback transaction')
            self.conn.rollback()
        
        del self.conn
    
    
    def get_tile(self, x,y, z):
        if self.alt_schema:
            self.curs.execute("select data from tile_data where x=? and y=? and z=?", (x,y,z))
        else:
            self.curs.execute("select tile_data from tiles where zoom_level=? and tile_column=? and tile_row=?",(z,x,y))
        tt=list(self.curs)
        if not tt:
            return None
        return tt[0][0]
    
    def __call__(self, x, y, z):
        if z<self.minzoom or z > self.maxzoom:
            return None
        self.ii+=1
        
        if self.flipy:
            y = (1<<z) - 1 - y
        
        ans=self.get_tile(x,y,z)
        if ans is None:
            return self.add_tile(x,y,z)
        
        return ans
    
    
    def start(self):
        if self.trans:
            self.conn.commit()
        if self.newconn:
            self.conn=sqlite3.connect(self.fn,check_same_thread=False)
            self.curs=self.conn.cursor()
        self.curs.execute("begin")
        self.trans=True
    def finish(self):
        self.conn.commit()
        self.trans=False
        
        
    def add_tile(self, x, y, z, data=None,basetile=None, tile=None, replace=False):
        if data is None:
            if self.gettile is None:
                #raise Exception("no gettile function specified")
                return None
            data = self.gettile(x,y,z)
            
        
        if self.alt_schema:
            if replace:
                self.curs.execute("delete from tile_data where x=? and y=? and z=?", (x,y,z))
            self.curs.execute("insert into tile_data values (?,?,?,?,?,?)", (basetile,tile,x,y,z,None if data is None else buffer(data)))
        else:
            if replace:
                self.curs.execute("delete from tile_data where tile_column=? and tile_row=? and zoom_level=?", (x,y,z))
            self.curs.execute("insert into tiles values (?,?,?,?)", (z,x,y,None if data is None else buffer(data)))
        if not self.trans:
            self.conn.commit()
        
        return data

class MbTilesCached(object):
    def __init__(self, mbtiles):
        self.mbtiles=mbtiles
        
        self.mbtiles.curs.execute("select tile_column,tile_row,zoom_level from tiles")
        self.cache = dict((tuple(c),None) for c in self.curs)
        self.ii=0
        self.nt=0
    
    def __call__(self, x,y,z):
        self.ii+=1
        if (x,y,z) in self.cache:
            if self.cache[x,y,z] is not None:
                self.cache[x,y,z][0]=ii
                return self.cache[x,y,z][1]
            
        data = self.mbtiles(x,y,z)
        self.cache[x,y,z]=[self.ii,data]
                    
        
        self.nt+=1
            
        if self.nt>1000:
            dl = sorted((data for data in self.tiles if self.tiles[data]), key=lambda k: self.tiles[k][0])[:100]
            for k in dl:
                self.tiles[k] = None
                self.nt-=1
        
        return data
