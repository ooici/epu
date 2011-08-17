#!/usr/bin/env python

"""
@file setup.py
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

import sys

setupdict = {
    'name' : 'epu',
    'version' : '0.4.3', #VERSION,
    'description' : 'OOICI CEI Elastic Processing Unit Services and Agents',
    'url': 'https://confluence.oceanobservatories.org/display/CIDev/Common+Execution+Infrastructure+Development',
    'download_url' : 'http://ooici.net/packages',
    'license' : 'Apache 2.0',
    'author' : 'CEI',
    'author_email' : 'tfreeman@mcs.anl.gov',
    'keywords': ['ooici','cei','epu'],
    'classifiers' : [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering'],
}

from setuptools import setup, find_packages
setupdict['packages'] = find_packages()

setupdict['dependency_links'] = ['http://ooici.net/releases']
setupdict['test_suite'] = 'epu'

# ssl package won't install on 2.6+, but is required otherwise.
# also, somehow the order matters and ssl needs to be before ioncore
# in this list (at least with setuptools 0.6c11).

setupdict['install_requires'] = []
if sys.version_info < (2, 6, 0):
    setupdict['install_requires'].append('ssl==1.15-p1')

setupdict['install_requires'] += ['simplejson==2.1.2',
                                  'httplib2==0.7.1',
                                  'nimboss==0.4.6',
                                  'txrabbitmq==0.5',
                                  'apache-libcloud==0.5.2',
                                  'ioncore==0.4.53']

setupdict['entry_points'] = {
        'console_scripts': [
            'epu-cassandra-schema=epu.cassandra:main'
            ]
        }

setup(**setupdict)
