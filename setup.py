from __future__ import print_function
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import io
import codecs
import os
import sys

import nzem

here = os.path.abspath(os.path.dirname(__file__))

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)

long_description = read('README.md')

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)

setup(
    name='nzem',
    version=nzem.__version__,
    url='http://github.com/NigelCleland/nzem',
    license='MIT Software License',
    author='Nigel Cleland',
    tests_require=['pytest'],
    install_requires=['pandas>=0.11.0',
                      'numpy>=1.7.1',
                      'requests>=1.2.3',
                      'beautifulsoup4>=4.3.1',
                      'sh>=1.08',
                      'simplejson>=3.3.0',
                      'matplotlib>=1.3.0'
                    ],
    cmdclass={'test': PyTest},
    author_email='nigel.cleland@gmail.com',
    description='Analysis tools for the NZ electricity market',
    long_description=long_description,
    packages=['nzem'],
    include_package_data=True,
    platforms='any',
    test_suite='nzem.test.test_nzem',
    classifiers = [
        'Programming Language :: Python',
        'Development Status :: 1 - Alpha',
        'Natural Language :: English',
        'Environment :: Data Analysis',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT Software License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        ],
    extras_require={
        'testing': ['pytest'],
    }
)
