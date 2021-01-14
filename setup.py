from setuptools import setup, Extension
import setuptools
import shutil, os.path, subprocess
import pybind11

def call_pkgconfig(args):
    try:
        ans = subprocess.getoutput(args)
        return ans.split()
    except:
        ans = subprocess.check_output(args.split(" "))
        return ans.split()

    
cflags = ['-std=c++17',]
#cflags.append('-I/home/james/work/oqtcpp/include')
cflags.append('-I/usr/local/include')
cflags.append('-fvisibility=hidden')
#cflags.append('-Wshadow')
libs =['-L/usr/local/lib', '-loqt', '-lsqlite3']

libs += [l for l in call_pkgconfig('mapnik-config --libs') if not l in libs]
cflags += [l for l in call_pkgconfig('mapnik-config --cflags') if not l in cflags]   

#mapnik cflags incompatiable with pybind11...
for x in ('-Wshadow','-std=c++11','-std=c++14'):
    if x in cflags:
        cflags.remove(x)


srcs = ['src/oqtsqlite.cpp','src/sqlite_funcs.cpp', 'src/result.cpp', 'src/sqlitedb.cpp', 'src/bindelement.cpp', 'src/mvt.cpp']

from distutils.command.build_ext import build_ext
from distutils.sysconfig import customize_compiler

class my_build_ext(build_ext):
    def build_extensions(self):
        customize_compiler(self.compiler)
        try:
            self.compiler.compiler_so.remove("-Wstrict-prototypes")
        except (AttributeError, ValueError):
            pass
        build_ext.build_extensions(self)

ext_modules = [
    Extension(
        'oqtsqlite._oqtsqlite',
        srcs,
        include_dirs=[
            '/usr/local/include',
            '/home/james/work/oqtcpp/include',
            pybind11.get_include(),
            
        ],
        extra_link_args=libs,
        extra_compile_args=cflags
        
    ),
    
]



setup(
    name='oqtsqlite',
    packages=['oqtsqlite'],
    version='0.0.1',
    long_description='',
    ext_modules=ext_modules,
    zip_safe=False,
    cmdclass = {'build_ext': my_build_ext}
)
