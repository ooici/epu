#!/usr/bin/env python

"""
@file setup.py
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

setupdict = {
    'name' : 'epu',
    'version' : '0.4.1', #VERSION,
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
#setupdict['include_package_data'] = True
#setupdict['package_data'] = {
#    'epu': ['data/*.sqlt', 'data/install.sh']
setupdict['install_requires'] = ['simplejson==2.1.2', 
                                 'httplib2==0.6.0',
                                 'nimboss==0.4.1',
                                 'txrabbitmq==0.5',
                                 'apache-libcloud==0.4.0',
                                 'ioncore==0.4.22']
setupdict['entry_points'] = {
        'console_scripts': [
            'epu-cassandra-schema=epu.cassandra:main'
            ]
        }

setup(**setupdict)
