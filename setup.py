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

version = '1.2.2-dev'

setupdict = {
    'name' : 'epu',
    'version' : version,
    'description' : 'OOICI CEI Elastic Processing Unit Services and Agents',
    'url': 'https://confluence.oceanobservatories.org/display/CIDev/Common+Execution+Infrastructure+Development',
    'download_url' : 'http://sddevrepo.oceanobservatories.org/releases',
    'license' : 'Apache 2.0',
    'author' : 'CEI',
    'author_email' : 'nimbus@mcs.anl.gov',
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

setupdict['dependency_links'] = ['http://sddevrepo.oceanobservatories.org/releases']
setupdict['test_suite'] = 'epu'

setupdict['install_requires'] = ['httplib2>=0.7.1',
                                  'boto >= 2.6',
                                  'apache-libcloud==0.11.1',
                                  'kazoo>=1.0',
                                  'dashi>=0.2.1',
                                  'gevent>=0.13.7',
                                  'simplejson',
                                  'mock'
                                 ]
setupdict['tests_require'] = ['epuharness', 'nose', 'mock']
setupdict['extras_require'] = {
        'test': setupdict['tests_require'],
        'exceptional': ['exceptional-python'],
        'statsd': ['statsd'],
        }
setupdict['test_suite'] = 'nose.collector'

setupdict['entry_points'] = {
        'console_scripts': [
            'epu-management-service=epu.dashiproc.epumanagement:main',
            'epu-provisioner-service=epu.dashiproc.provisioner:main',
            'epu-processdispatcher-service=epu.dashiproc.processdispatcher:main',
            'epu-zktool=epu.zkcli:main',
            'epu-high-availability-service=epu.dashiproc.highavailability:main',
            'epu-dtrs=epu.dashiproc.dtrs:main',
            ]
        }
setupdict['scripts'] = ["scripts/epu-process"]

setupdict['package_data'] = {'epu': ['config/*.yml']}

setup(**setupdict)
