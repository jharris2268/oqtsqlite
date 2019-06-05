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

import itertools
import re

# This is the mataclass-defined __init__
def auto_init(self, *args, **kwargs):
    for arg_val, arg_name in itertools.izip_longest(args, self.params):
        if arg_name is None:
            raise Exception("too many arguments... for "+self.name)
        setattr(self, arg_name, arg_val)
    
    # This would allow the user to explicitly specify field values with named arguments
    self.__dict__.update(kwargs)

def auto_repr(self):
    return "%s(%s)" % (self.name, ", ".join(repr(getattr(self,s)) for s in self.params))
    

class MetaBase(type):
    def __new__(cls, name, bases, attrs):
        attrs['__init__'] = auto_init
        attrs['__repr__'] = auto_repr
        return super(MetaBase, cls).__new__(cls, name, bases, attrs)

class Base(object):
    __metaclass__ = MetaBase

zoom0 = 156543.033928125

### Values ###

class StringValue(Base):
    name = 'StringValue'
    params = ['Value']
    
    
    @property
    def Key(self):
        return ''
    
    @property
    def sqlite(self):
        return "'%s'" % self.Value
    
    def __call__(self, feat, zoom):
        return self.Value

class IntegerValue(Base):
    name = 'IntegerValue'
    params = ['Value']
    
    @property
    def Key(self):
        return ''
    
    @property
    def sqlite(self):
        return '%d' % self.Value
    
    def __call__(self, feat, zoom):
        return self.Value

class FloatValue(Base):
    name = 'FloatValue'
    params = ['Value']
    
    @property
    def Key(self):
        return ''
    
    @property
    def sqlite(self):
        return '%f' % self.Value
    
    def __call__(self, feat, zoom):
        return self.Value

class NullValue(Base):
    name = 'NullValue'
    params = []
    
    @property
    def Key(self):
        return ''
    
    @property
    def sqlite(self):
        return 'NULL'
    
    def __call__(self, feat, zoom):
        return None

### Columns ###

class Label(Base):
    name = 'Label'
    params = ['Key']

    @property
    def sqlite(self):
        if re.match('^\w+$', self.Key):
            return self.Key
        return '"%s"' % self.Key

    def __call__(self, feat, zoom):
        if self.Key in feat:
            return feat[self.Key]
        return None

class AsLabel(Base):
    name = 'AsLabel'
    params = ['Key','Column']
    
    @property
    def sqlite(self):
        return '%s AS "%s"' % (self.Column.sqlite, self.Key)
    
    def __call__(self, feat, zoom):
        return self.Column(feat,zoom)

class AsString(Base):
    name = 'AsString'
    params = ['Column']
    
    @property
    def Key(self):
        return self.Column.Key
    
    @property
    def sqlite(self):
        return 'CAST (%s AS TEXT)' % (self.Column.sqlite,)
    
    def __call__(self, feat, zoom):
        a = self.Column(feat,zoom)
        if a is None:
            return None
        
        return str(a)

class AsInteger(Base):
    name = 'AsInteger'
    params = ['Column']
    
    @property
    def Key(self):
        return self.Column.Key
    
    @property
    def sqlite(self):
        return 'CAST (%s AS INTEGER)' % (self.Column.sqlite,)
    
    def __call__(self, feat, zoom):
        a = self.Column(feat,zoom)
        if a is None:
            return None
        
        try:
            return int(a)
        except:
            return None
        
class AsFloat(Base):
    name = 'AsFloat'
    params = ['Column']
    
    @property
    def Key(self):
        return self.Column.Key
    
    @property
    def sqlite(self):
        return 'CAST (%s AS FLOAT)' % (self.Column.sqlite,)
    
    def __call__(self, feat, zoom):
        a = self.Column(feat,zoom)
        if a is None:
            return None
        
        try:
            return float(a)
        except:
            return None
            
class Case(Base):
    name = 'Case'
    params = ['Clauses']
    
    @property
    def Key(self):
        if self.Clauses:
            return self.Clauses[0].Column.Key
        
        return ''
        
    @property
    def sqlite(self):
        return "CASE %s END" % (' '.join(cl.sqlite for cl in self.Clauses),)
    
    def __call__(self, feat, zoom):
        for cl in self.Clauses:
            a,b = cl(feat, zoom)
            if a:
                return b
            
        return None

class CaseWhen(Base):
    name = 'CaseWhen'
    params = ['Where','Column']
    
    @property
    def sqlite(self):
        return 'WHEN %s THEN %s' % (self.Where.sqlite, self.Column.sqlite)
        
    def __call__(self, feat, zoom):
        if not self.Where(feat, zoom):
            return False, None
        
        return True, self.Column(feat, zoom)

class CaseElse(Base):
    name = 'CaseElse'
    params = ['Column']
    
    @property
    def sqlite(self):
        return 'ELSE %s' % (self.Column.sqlite, )
    
    def __call__(self, feat, zoom):
        return True, self.Column(feat, zoom)

class StringLength(Base):
    name = 'StringLength'
    params = ['Column']
    
    @property
    def Key(self):
        return 'stringlength'
    
    @property
    def sqlite(self):
        return 'length(%s)' % (self.Column.sqlite,)
        
    def __call__(self, feat, zoom):
        col = self.Column(feat, zoom)
        if not isinstance(col, basestring):
            return None
        
        return len(col)


class Coalesce(Base):
    name = 'Coalesce'
    params = ['Values']
    
    @property
    def Key(self):
        return 'coalesce'
    
    @property
    def sqlite(self):
        return 'coalesce(%s)' % (", ".join(v.sqlite for v in self.Values),)
    
    def __call__(self, feat, zoom):
        for v in self.Values:
            vl = v(feat, zoom)
            if not vl is None:
                return vl
        
        return None

class NullIf(Base):
    name = 'NullIf'
    params = ['Left','Right']
    
    @property
    def Key(self):
        return self.Left.Key
    
    @property
    def sqlite(self):
        return 'nullif(%s, %s)' % (self.Left.sqlite, self.Right.sqlite)
    
    def __call__(self, feat, zoom):
        left = self.Left(feat,zoom)
        right = self.Right(feat,zoom)
        
        if left==right:
            return None
        
        return left


class Concat(Base):
    name = 'Concat'
    params = ['Values', 'AllowNulls']
    
    @property
    def Key(self):
        if self.Values:
            return self.Values[0].Key
        
    @property
    def sqlite(self):
        if self.AllowNulls:
            return ' || '.join("coalesce(%s, '')" % (v.sqlite,) for v in self.Values)
        
        return ' || '.join(v.sqlite for v in self.Values)
    
    
    def __call__(self, feat, zoom):
        res = ''
        for v in self.Values:
            vl = v(feat, zoom)
            if vl is None:
                if not self.AllowNulls:
                    return None
            else:
                res += vl
        
        return res

class ColumnOp(Base):
    name = 'ColumnOp'
    params = ['Left','Op','Right']
    
    @property
    def Key(self):
        return '"%s%s%s"' % (self.Left.Key, self.Op, self.Right.Key)
    
    @property
    def sqlite(self):
        return "%s %s %s" % (self.Left.sqlite, self.Op, self.Right.sqlite)
        
    def __call__(self, feat, zoom):
        
        left= self.Left(feat, zoom)
        op = self.Op
        right= self.Left(feat, zoom)
        
        if left is None or op is None or right is None:
            return None
        
        if op == '+':
            return left+right
        
        if op == '-':
            return left+right
        
        if op == '*':
            return left+right
        
        if op == '/':
            return left+right
        
        if op == '%':
            return left+right
        
class PixelArea(Base):
    name = 'PixelArea'
    params = []
    
    @property
    def Key(self):
        return 'pixel_area'
    
    @property
    def sqlite(self):
        return ':pixel_area:'
    
    def __call__(self, feat, zoom):
        return (zoom0 / 2.0**zoom) ** 2
    
class PixelSize(Base):
    name = 'PixelSize'
    params = []
    
    @property
    def Key(self):
        return 'pixel_size'
    
    @property
    def sqlite(self):
        return ':pixel_size:'
    
    def __call__(self, feat, zoom):
        return (zoom0 / 2.0**zoom) 


class ScaleDenominator(Base):
    name = 'ScaleDenominator'
    params = []
    
    @property
    def Key(self):
        return 'scale_denominator'
    
    @property
    def sqlite(self):
        return ':scale_denominator:'
    
    def __call__(self, feat, zoom):
        return (zoom0 / 2.0**zoom) / 0.00028

class Power(Base):
    name = 'Power'
    params = ['Left','Right']
    
    @property
    def Key(self):
        return 'power'
    
    @property
    def sqlite(self):
        return "power(%s, %s)" % (self.Left.sqlite, self.Right.sqlite)
    
    def __call__(self, feat, zoom):
        l = self.Left(feat,zoom)
        r = self.Right(feat,zoom)
        return l**r
        
        
class SubStr(Base):
    name = 'SubStr'
    params = ['Column','From','Len']
    
    @property
    def Key(self):
        return self.Column.Key
    
    @property
    def sqlite(self):
        return 'substr(%s, %s%s)' % (self.Column.sqlite, self.From.sqlite, '' if self.Len is None else ", "+self.Len.sqlite)
        
    def __call__(self, feat, zoom):
        vl = self.Column(feat, zoom)
        if not isinstance(vl, basestring):
            return None
        
        fr = self.From(feat, zoom)
        if fr is None:
            return None
        vl = vl[fr:]
        
        if not self.Len is None:
            ln = self.Len(feat, zoom)
            if ln is None:
                return None
            return vl[:ln]
        
        return vl


class HstoreAccess(Base):
    name = 'HstoreAccess'
    params = ['Column', 'Key']
    
    @property
    def sqlite(self):
        return "hstore_access(%s, '%s')" % (self.Column.sqlite, self.Key)
    
    def __call__(self, feat, zoom):
        col  =self.Column(feat, zoom)
        if not self.Key in feat:
            return None
        
        return feat[self.Key]
    

class AsTable(Base):
    type = 'Table'
    name = 'AsTable'
    params = ['Table','Alias']
    
    @property
    def sqlite(self):
        return "(%s) AS %s" % (self.Table.sqlite, self.Alias.sqlite)
    
    def __call__(self, data, zoom):
        return self.Table(data,zoom)

class Union(Base):
    type = 'Table'
    name = 'Union'
    params = ['Left', 'Right']
    
    @property
    def sqlite(self):
        return "%s UNION %s" % (self.Left.sqlite, self.Right.sqlite)
        
    def __call__(self, data, zoom):
        
        return self.Left(data,zoom)+self.Right(data, zoom)
    
    
    

class Select(Base):
    type = 'Table'
    name='Select'
    params = ['Columns','Table','Where','OrderBy']

    @property
    def sqlite(self):
        res = "SELECT %s FROM %s" % (self.Columns.sqlite, self.Table.sqlite)
        if not self.Where is None:
            res += " WHERE %s" % (self.Where.sqlite)
        if not self.OrderBy is None:
            res += " "+self.OrderBy.sqlite
        
        
        
        return res
    
    def __call__(self, data, zoom):
        
        feats = self.Table(data, zoom)
        if self.Where:
            feats = [f for f in feats if self.Where(f, zoom)]
        
        feats = [(self.Columns(f,zoom),f) for f in feats]
        
        if self.OrderBy:
            feats.sort(cmp=lambda x,y: self.OrderBy(x, y, zoom))
        
        return [a for a,b in feats]


class AllColumns(Base):
    name = 'AllColumns'
    params = []
    
    @property
    def sqlite(self):
        return '*'
    
    def __call__(self, feature, zoom):
        return feature
        
        
def check_col_sqlite(col):
    if col.name=='Case':
        key=col.Key
        if key:
            return '%s AS %s' % (col.sqlite, key)
    
    return col.sqlite    
    

class ColumnList(Base):
    name = 'ColumnList'
    params = ['Columns']
    
    @property
    def sqlite(self):
        
        return ", ".join(check_col_sqlite(col) for col in self.Columns)

    def __call__(self, feature, zoom):
        
        ans = {}
        for col in self.Columns:
            ans[col.Key] = col(feature, zoom)
        
        return ans

class PickTable(Base):
    type = 'Table'
    name = 'PickTable'
    params = ['Table']
    
    @property
    def sqlite(self):
        return self.Table
    
    
    def __call__(self, data, zoom):
        if not self.Table in data:
            return []
        return data[self.Table]

### Where statement parts

class And(Base):
    type = 'Where'
    name='And'
    params = ['Left','Right']
    
    @property
    def sqlite(self):
        return "(%s AND %s)" % (self.Left.sqlite, self.Right.sqlite)
    
    def __call__(self, feat, zoom):
        return self.Left(feat, zoom) and self.Right(feat, zoom)

class Or(Base):
    type = 'Where'
    name='Or'
    params = ['Left','Right']
    
    @property
    def sqlite(self):
        return "(%s OR %s)" % (self.Left.sqlite, self.Right.sqlite)
    
    def __call__(self, feat, zoom):
        return self.Left(feat, zoom) or self.Right(feat, zoom)

class Not(Base):
    type = 'Where'
    name='Not'
    params = ['Where']
    
    @property
    def sqlite(self):
        return "(NOT %s)" % (self.Where.sqlite,)
    
    def __call__(self, feat, zoom):
        return not self.Where(feat, zoom)


class IsNull(Base):
    type = 'Where'
    name = 'IsNull'
    params = ['Where']
    
    @property
    def sqlite(self):
        return "(%s IS NULL)" % (self.Where.sqlite,)
    
    def __call__(self, feat, zoom):
        return self.Where(feat, zoom) is None

class In(Base):
    type = 'Where'
    name = 'In'
    params = ['Column','Values' ]
    
    @property
    def sqlite(self):
        return "(%s IN (%s))" % (self.Column.sqlite,", ".join(v.sqlite for v in self.Values))
    
    def __call__(self, feat, zoom):
        col = self.Column(feat, zoom)
        for vl in self.Values:
            v = vl(feat,zoom)
            if col==vl:
                return True
        
        return False

class HstoreContains(Base):
    type = 'Where'
    name = 'HstoreContains'
    params = ['Column', 'Key', 'Val']
    
    @property
    def sqlite(self):
        ks = "'%s'" % self.Key
        vs = ''
        if self.Val is not None:
            vs = ", '%s'" % self.Val
        return "hstore_contains(%s, %s%s)" % (self.Column.sqlite, ks, vs)
    
    def __call__(self, feat, zoom):
        col = self.Column(feat, zoom)
        if not isinstance(col, dict):
            return False
        
        if not self.key in col:
            return False
        
        if self.val and col[self.key]==self.val:
            return True
        return False
        
class Op(Base):
    type = 'Where'
    name = "Op"
    params = ['Left', 'Op', 'Right']
    
    @property
    def sqlite(self):
        return "(%s %s %s)" % (self.Left.sqlite, self.Op, self.Right.sqlite)
    
    def __call__(self, feat, zoom):
        left = self.Left(feat, zoom)
        op = self.Op
        right = self.Right(feat, zoom)
        
        if left is None or op is None or right is None: return False
        
        if op in ('=', '=='):
            return left==right
        if op == '<':
            return left < right
        if op == '<=':
            return left <= right
        if op == '>':
            return left > right
        if op == '>=':
            return left >= right
        if op in ('!=', '<>'):
            return left != right
        
        raise Exception("unexpected operator %s" % (repr(op),))

class RegEx(Base):
    type = 'Where'
    name = "RegEx"
    params = ['Column', 'Match']
    
    @property
    def sqlite(self):
        return "regex_call(%s, '%s')" % (self.Column.sqlite, self.Match)
        
    def __call__(self,feat,zoom):
        col = self.Column(feat,zoom)
        if not col or not isinstance(col,basestring):
            return False
        
        return bool(re.match(self.Match, col))
    
        
#### Array Functions ####

class ArrayLength(Base):
    name='ArrayLength'
    params = ['Column']
    
    @property
    def sqlite(self):
        return "array_length(%s)" % (self.Column.sqlite,)
    
    def __call__(self, feat, zoom):
        col = self.Column(feat,zoom)
        if not isinstance(col, list):
            return None
        return len(col)

class ArrayToString(Base):
    name = 'ArrayToString'
    params = ['Column', 'Seperator']
    
    @property
    def sqlite(self):
        return "array_to_string(%s, '%s')" % (self.Column.sqlite,self.Seperator)
    
    def __call__(self, feat, zoom):
        col = self.Column(feat,zoom)
        if not isinstance(col, list):
            return None
        
        sep = self.Seperator(feat,zoom)
        if not isinstance(sep, basestring):
            return None
        
        return sep.join(col)
    
    
class StringToArray(Base):
    name = 'StringToArray'
    params = ['Column', 'Seperator']
    
    @property
    def sqlite(self):
        return "string_to_array(%s, '%s#')" % (self.Column.sqlite,self.Seperator)
    
    def __call__(self, feat, zoom):
        col = self.Column(feat,zoom)
        if not isinstance(col, basestring):
            return None
        
        sep = self.Seperator(feat,zoom)
        if not isinstance(sep, basestring):
            return None
        
        return col.split(sep)

class MaxEntryLength(Base):
    name = 'MaxEntryLength'
    params = ['Column']
    
    @property
    def sqlite(self):
        return "max_entry_length(%s)" % (self.Column.sqlite,)
    
    def __call__(self, feat, zoom):
        col = self.Column(feat,zoom)
        if not isinstance(col, list):
            return None
        return max(len(c for c in col))

class OrderItem(Base):
    type = 'OrderItem'
    name = 'OrderItem'
    params = ['Column','Asc']
    
    @property
    def sqlite(self):
        return '%s%s' % (self.Column.sqlite, '' if self.Asc else ' DESC')
    
    def __call__(self, left, right, zoom):
        l = self.Column(left)
        r = self.Column(right)
        
        c = cmp(l,r)
        if not self.Asc:
            c *= -1
        
        return c

class OrderBy(Base):
    type = 'OrderBy'
    name = 'OrderBy'
    params = ['Items']
    
    @property
    def sqlite(self):
        return "ORDER BY %s" % (", ".join(oi.sqlite for oi in self.Items),)
        
    def __call__(self, left, right, zoom):
        for oi in self.Items:
            c = oi(left,right,zoom)
            if c!=0:
                return c
        return 0
        
        
