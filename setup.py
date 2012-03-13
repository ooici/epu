#!/usr/bin/env python

"""
@file setup.py
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

import sys
import os

# Add /usr/local/include to the path for macs, fixes easy_install for several packages (like gevent and pyyaml)
if sys.platform == 'darwin':
    os.environ['C_INCLUDE_PATH'] = '/usr/local/include'

setupdict = {
    'name' : 'epu',
    'version' : '1.2.0',
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

setupdict['install_requires'] += ['simplejson>=2.3.3',
                                  'httplib2==0.7.1',
                                  'nimboss==0.4.6',
                                  'apache-libcloud==0.8.0',
                                  'dashi==0.1',
                                  'gevent==0.13.6',
                                  'nose',
                                  'mock',
                                  'faulthandler',
                                 ]
setupdict['tests_require'] = ['epuharness']
setupdict['extras_require'] = {'test': setupdict['tests_require']}
setupdict['test_suite'] = 'nose.collector'

setupdict['entry_points'] = {
        'console_scripts': [
            'epu-management-service=epu.dashiproc.epumanagement:main',
            'epu-provisioner-service=epu.dashiproc.provisioner:main',
            'epu-processdispatcher-service=epu.dashiproc.processdispatcher:main',
            'epu-worker=epu.dashiproc.epu_worker:main',
            'epu-high-availability-service=epu.dashiproc.highavailability:main',
            ]
        }
setupdict['scripts'] = ["scripts/epu-process"]

setupdict['package_data'] = {'epu': ['config/*.yml']}

setup(**setupdict)
