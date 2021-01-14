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

from __future__ import unicode_literals, print_function
from arpeggio import *
from arpeggio import RegExMatch as _
import collections, json
from . import sqlparts as sq

def get_comparision_op(opf):
    return opf

def get_arithmitic_op(opf):
    return opf

class IKwd(StrMatch):
    """
    A specialization of StrMatch to specify keywords of the language.
    """
    def __init__(self, to_match):
        super(IKwd, self).__init__(to_match,ignore_case=True)
        self.to_match = to_match
        self.root = True
        self.rule_name = 'keyword'

def number():       return _(r'\-?(\d+\.\d*|\d+)')
def strliteral():   return _(r'[eE]?\'(?:[^\']|\'\')*\''),Optional("::text")
def unq_label():    return _(r'[a-z|A-Z][a-z|A-Z|0-9|_]*')
def qq_label(): return '"',_(r'[a-z|A-Z][a-z|A-Z|0-9|_|:]*'),'"'
def label():        return Optional([StrMatch("p."),StrMatch("l.")]),[unq_label,qq_label]#,Optional("::",unq_label)

def substring_weird(): return IKwd("substring"),"(",label,IKwd("for"),number,")"
def function():     return unq_label,"(",Optional(valuelist),")"
def in_value(): return label,IKwd("in"),"(",Optional(valuelist),")"

def calculation():  return [StrMatch(x) for x in ('->>','->','||','*','+','-','/','%',)]
def operation():    return [StrMatch(x) for x in ('==','<>','<=','>=','<','>','=','!=','~','?','@>',)]



def case_value():    return IKwd("case"), OneOrMore(case_when), Optional(case_else), IKwd("end")
def case_when():    return IKwd("when"), wherefield, IKwd("then"), value
def case_weird_when():    return IKwd("when"), value, IKwd("then"), value
def case_else():    return IKwd("else"), value
def case_weird_else():    return IKwd("else"), value
def case_weird():   return IKwd("case"), label, OneOrMore(case_weird_when), Optional(case_weird_else), IKwd("end")



def null():         return IKwd("null")
def literal_value(): return [number,strliteral]
def value():        return ([literal_value,case_value,case_weird,substring_weird,function,null,label,("(",value,")")],Optional(calculation,value)),Optional([StrMatch("::"),],unq_label)#,in_value)
def valuelist():    return value,ZeroOrMore(",",value)

def asfield():      return value, IKwd("as"), [unq_label,qq_label]
def field():        return [asfield,value,label]

def allfield():     return StrMatch("*")
def fieldlist():    return (field, ZeroOrMore(",", field))


def table():        return [(unq_label,Optional(["p","l"])),("(",bothselect,")", Optional([(IKwd("as"), label),'_']))]
def where():        return IKwd("where"), wherefield
def labellist_brackets():   return "(", unq_label, ZeroOrMore(",", unq_label), ")"
def simple_valuelist_brackets():   return "(", [number,strliteral], ZeroOrMore(",",[number,strliteral]),")"
def valuestable():  return "(", IKwd("values"), simple_valuelist_brackets, ZeroOrMore(",",simple_valuelist_brackets), ")", IKwd("as"), unq_label, labellist_brackets

def wherefield():  return wherefield_entry, Optional(where_join)
def wherefield_entry(): return [binary_comp, unary_comp, where_in, bracket_wherefield,not_wherefield]
def bracket_wherefield(): return "(",wherefield,")"
def not_wherefield(): return IKwd("not"), wherefield

def where_in():     return Optional(IKwd("not")),field, Optional(IKwd("not")), IKwd("in"), "(", literal_value, ZeroOrMore(",",literal_value), ")"
def where_join():   return [IKwd("and"), IKwd("or")], wherefield

def binary_comp():  return Optional(IKwd("not")), value, operation, value
def unary_comp():   return value, IKwd("is"), Optional(IKwd("not")), IKwd("null")

def order_by():     return IKwd("order"),IKwd("by"), order_term, ZeroOrMore(",", order_term)
def order_term():   return value, Optional([IKwd("asc"),IKwd("desc")]), Optional(IKwd("nulls"),[IKwd("first"),IKwd("last")])


def select():       return IKwd("select"),[fieldlist,allfield],IKwd("from"),table,Optional(where),Optional(order_by)
def unionselect():  return select,IKwd("union"),Optional(IKwd("all")),select
def bothselect():   return [unionselect,select]

def stmt():         return [bothselect], EOF


class Visitor(PTNodeVisitor):
    def visit_number(self,n,c):
        try:
            return sq.IntegerValue(int(str(n)))
        except:
            return sq.FloatValue(float(str(n)))
    
    
    
    def visit_strliteral(self,n,c):
        r= str(c[0]) if c else str(n)
        if r[:2] in ("e'", "E'") and r[-1] == "'":
            r = eval(r[1:])
        if r[0]=="'" and r[-1]=="'":
            r =  r[1:-1]
        return sq.StringValue(r)
    
    def visit_literal_value(self, n, c):
        return c[0]
    
    def visit_null(self,n,c):
        return sq.NullValue()
    
    def visit_qq_label(self,n,c):
        return c[0]
    def visit_unq_label(self,n,c):
        return str(n)

    def visit_label(self,n,c):
        if c[0] in ('l.','p.'):
            
            c = [c[0]+c[1]]+c[2:]
        
        if c[0].lower()=='true':
            #return sq.IntegerValue(1)
            return sq.BoolValue(True)
        elif c[0].lower()=='false':
            #return sq.IntegerValue(0)
            return sq.BoolValue(False)
        ll = sq.Label(c[0])
            
        return ll



    def visit_value(self,n, c):
        if len(c)==1:
            if type(c[0])==str:
                return sq.Label(c[0])
            return c[0]
        if len(c)!=3:
            
            raise Exception("??, visit_value "+repr(n)+" "+repr(c))
        if c[1]=='||':
            return sq.Concat([c[0],c[2]],False)
        elif c[1] in ('->','?','@>','->>'):
            qq= sq.HstoreAccess(c[0], c[2](None,0))
            
            return qq
        elif c[1]=='::':
            
            ll,cc=c[0],c[2]
            if str(cc).lower() == 'integer':
                return sq.AsInteger(ll)
            
            elif str(cc).lower() == 'numeric':
                return sq.AsFloat(ll)
            elif str(cc).lower() == 'text':
                return sq.AsString(ll)
            
            raise Exception("type cast ?? "+cc)
            
        else:
                
            return sq.ColumnOp(c[0],get_arithmitic_op(c[1]),c[2])

    def visit_asfield(self,n,c):
        if len(c)!=2:
            raise Exception("visit_asfield",repr(n),len(c),repr(c))
        
        return sq.AsLabel(c[1],c[0])

    def visit_allfield(self,n,c):
        return sq.AllColumns()

    def visit_case_when(self,n,c):
        return sq.CaseWhen(c[0],c[1])
    
    def visit_case_else(self,n,c):
        return sq.CaseElse(c[0])

    def visit_case_value(self,n,c):
        return sq.Case(c)

    def visit_case_weird_when(self,n,c):
        raise Exception('case_weird')
        
    def visit_case_weird_else(self,n,c):
        raise Exception('case_weird')
        

    def visit_case_weird(self,n,c):
        raise Exception('case_weird')
        
    def visit_in_value(self, n, c):
        print("visit_in_value",n,c)
        return sq.InValue(c[0],c[1])

    def visit_function(self,n,c):
        fn = str(c[0]).lower()
        if fn == 'coalesce':
            return sq.Coalesce(c[1])
        elif fn == 'concat':
            return sq.Concat(c[1],True)
        
        elif fn == 'nullif':
            return sq.NullIf(c[1][0],c[1][1])
        elif fn == 'pixel_area':
            return sq.PixelArea()
        elif fn == 'pixel_size':
            return sq.PixelSize()
        elif fn == 'scale_denominator':
            return sq.ScaleDenominator()
        
            
        elif fn in ('char_length', 'length'):
            return sq.StringLength(c[1][0])
        
        
        elif fn == 'substr':
            
            return sq.SubStr(c[1][0],c[1][1],c[1][2])
        elif fn == 'substring':
            return sq.SubStr(c[1][0],sq.IntegerValue(0),sq.StringLength(c[1][1]))
        
        
        elif fn == 'array_to_string':
            
            return sq.ArrayToString(c[1][0],c[1][1](None,0))
        elif fn == 'string_to_array':
            return sq.StringToArray(c[1][0],c[1][1](None,0))
        elif fn == 'max_entry_length':
            return sq.MaxEntryLength(c[1][0])
        elif fn == 'array_length':
            return sq.ArrayLength(c[1][0])
        
        elif fn == 'round':
            return sq.AsInteger(c[1][0])
            
        elif fn == 'pow':
            return sq.Power(c[1][0], c[1][1])
        
        elif fn == 'md5':
            return sq.StringValue('c')
        elif fn == 'ascii':
            return sq.IntegerValue(99)
        
        elif fn == 'check_bbox':
            return sq.CheckBBox()
            
        elif fn == 'st_pointonsurface':
            return sq.AsPoint(c[1][0])
        
        print('other func', fn)
    
    def visit_substring_weird(self,n,c):
        #print('substring_weird',c)
        return sq.SubStr(c[0],sq.IntegerValue(0),c[1])


    def visit_fieldlist(self,n,c):
        return sq.ColumnList(c)

    def visit_valuelist(self,n,c):
        return list(c)

    def visit_table(self,n,c):
        if len(c)==1:
            if hasattr(c[0],'type') and c[0].type=='Table':
                return c[0]
            return sq.PickTable(c[0])

        if len(c)==2:
            if hasattr(c[0],'type') and c[0].type=='Table':
                if c[1] == '_':
                    return c[0]
                return sq.AsTable(c[0],c[1])
            if c[1] in ('p','l'):
                return sq.PickTable(c[0])
            
        raise Exception("???", 'visit_table', [str(z)[:25] for z in c])
        
        
        #print 'visit_table', map(repr,c)
        #return c[0]
        left = c[0]
        if not hasattr(left,'type'):
            left=sq.PickTable(left)
        
        
        return sq.Join(left, c[2], map(sq.Label,c[3]), '')


    def visit_not_wherefield(self, n, c):
        #print 'visit_not_wherefield', c
        return sq.Not(c[0])
        
    def visit_whereand(self,n,c):
        return sq.And(c[0],c[1])
    def visit_whereor(self,n,c):
        return sq.Or(c[0],c[1])
        
    def visit_binary_comp(self,n,c):
        
        if len(c)==4 and str(c[0])=='not':
            return sq.Not(self.visit_binary_comp(n,c[1:]))
            #opf = get_comparision_op(c[2])
            #return sq.Not(sq.Op(c[1],opf,c[3]))
        
        if c[1]=='~':
            return sq.RegEx(c[0],c[2](None,0))
        if c[1] in ('->','?','@>','->>'):
            
            if c[1]=='?':
                return sq.HstoreContains(c[0],c[2](None,0), None)
                
            elif c[1]=='@>':
                nn = c[2](None,0)
                
                if '=>' in nn:
                    k,v=nn.split("=>")
                    return sq.HstoreContains(c[0],k,v)
                else:
                    raise Exception("??? visit_binary_comp %s {%d} %s" % (repr(n),len(c),repr(c)))
                
            else:
                raise Exception("??? visit_binary_comp %s {%d} %s" % (repr(n),len(c),repr(c)))
        opf = get_comparision_op(c[1])
        
        return sq.Op(c[0],opf,c[2])

    def visit_unary_comp(self,n,c):
        if len(c)==1:
            return sq.IsNull(c[0])
        if len(c)==2 and str(c[1])=="not":
            return sq.Not(sq.IsNull(c[0]))

        raise Exception("??? visit_unary_comp %s {%d} %s" % (repr(n),len(c),repr(c)))

    def visit_bracketwhere(self, n, c):
        #print 'visit_bracketwhere', len(c)
        return c[0]

    def visit_where_join(self, n, c):
        #print 'where_join', len(c), c[0],c[1]
        return c

    def visit_wherefield(self, n, c):
        #print 'visit_wherefield', n, len(c), c[1] if len(c)>1 else None
        if len(c)==1:
            return c[0]
        a,b = c[1]
        if a=='and':
            return sq.And(c[0],b)
        elif a=='or':
            return sq.Or(c[0],b)
        raise Exception("??? visit_wherefield %s %s" % (repr(n),repr(c)))

    def visit_where(self,n,c):
        #print 'visit_where', repr(n), repr(c)
        return c[0]

    def visit_where_in(self,n,c):
        isnot=False
        lab = c[0]
        if str(lab)=='not':
            isnot=True
            lab=c[1]
        elif str(c[1]) == 'not':
            isnot=True
        i=1
        if isnot: i=2
        vals = c[i:]

        inv = sq.In(lab,vals)
        if isnot:
            return sq.Not(inv)
        return inv


    def visit_order_by(self,n,c):
        return sq.OrderBy(list(c))
        
        

    def visit_order_term(self,n,c):
        return sq.OrderItem(c[0], c[1]!='desc' if len(c)>1 else True)

    def visit_labellist_brackets(self,n,c):
        return list(c)

    def visit_simple_valuelist_brackets(self,n,c):
        #print('visit_simple_valuelist_brackets', n, c)
        return list(c)

    def visit_valuestable(self,n,c):
        #print 'visit_valuestable', repr(n), len(c), repr(c)
        raise Exception("visit_valuestable")
        
        vals = c[:-2]
        nn, cols=c[-2:]
        feats=sq.FeatureVec()
        for i,v in enumerate(vals):
            p=sq.KeyValueMap()
            for kk,vv in zip(cols,v):
                p[kk]=vv
            #print map(type,[1,p,0,str("")])
            f=sq.make_feature(1,p,0,str(""))
            #print f
            feats.append(f)
            
        
        return sq.ValuesTable(feats, nn)
        
        

    def visit_select(self,n,c):
        
        if len(c)==1 and (isinstance(c[0],sq.Select) or isinstance(c[0], sq.Union)): return c[0]
        
        
        cols = c[0]
        table = c[1]
        where = None
        order = None
        
        
        
        i=2
        if i<len(c) and hasattr(c[i],'type') and c[i].type == 'Where':
            
            where=c[i]
            i+=1
        if i<len(c) and hasattr(c[i],'type') and c[i].type == 'OrderBy':
            
            order=c[i]
            i+=1
            
        if i<len(c):
            print('?? select tail', c[i:])
        
        
        return sq.Select(cols,table,where,order)
    
    def visit_unionselect(self, n, c):
        
        left=c[0]
        right=c[-1]
        return sq.Union(left,right)
        
    def visit_bothselect(self, n, c):
        
        return c[0]
    
    
    
    




def clean_query(s):
    s="\n".join(l[:l.find('--')] if '--' in l else l for l in s.split("\n"))
    s=s.replace("!pixel_width!*!pixel_height!","pixel_area()")
    s=s.replace("!pixel_width!::real*!pixel_height!::real","pixel_area()")
    s=s.replace("!pixel_area!::real","pixel_area()")
    s=s.replace("!pixel_area!","pixel_area()")
    s=s.replace(":pixel_area:","pixel_area()")
    s=s.replace("!pixel_width!::real","pixel_size()")
    s=s.replace("!pixel_width!","pixel_size()")
    s=s.replace("!pixel_height!","pixel_size()")    
    s=s.replace("!scale_denominator!::real","scale_denominator()")
    s=s.replace("!scale_denominator!","scale_denominator()")
    s=s.replace("way && !bbox!", "check_bbox()==True")
    
    #s=s.replace("layer~E'^-?\\\\d+$'","layer is not null")
    #s=s.replace("ele ~ \'^-?\\d{1,4}(\\.\\d+)?$\'", "ele is not null")
    #s=s.replace("population ~ '^[0-9]{1,8}$'", "population is not null")
    s=s.replace("CONCAT(REPLACE(ROUND(elevation)::TEXT, \'-\', U&\'\\2212\'), U&\'\\00A0\', \'m\')","CONCAT(elevation::TEXT,'m')")
    #s=s.replace("E\'\\n\'","'\\n'")
    s=s.replace("(SELECT MAX(char_length(ref)) FROM unnest(refs) AS u(ref))", "max_entry_length(refs)")

    return s

parser = ParserPython(stmt)
visitor = Visitor()
def proc(qu, drop_wrapper=True):
    clean = clean_query(qu.strip())
    if clean.startswith("("):
        clean = "select * from "+clean
    
    res = visit_parse_tree(parser.parse(clean), visitor)
    if drop_wrapper:
        if isinstance(res, sq.Select):
            if isinstance(res.Columns, sq.AllColumns) and not res.Where and not res.OrderBy:
                if isinstance(res.Table, sq.AsTable):
                    return res.Table.Table
                return res.Table

    return res


def as_json(query):
    if isinstance(query, list):
        return [as_json(q) for q in query]
        
    if not isinstance(query, sq.Base):
        return query
    
        
    result = {}
    result['name'] = query.name
    if hasattr(query, 'type'):
        result['type'] = query.type
    for p in query.params:
        result[p] = as_json(getattr(query, p))
        
    return result
