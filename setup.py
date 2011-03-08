#!/usr/bin/env python

"""
@file setup.py
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

setupdict = {
    'name' : 'cei',
    'version' : '0.3.0', #VERSION,
    'description' : 'OOICI CEI Services and EPU Agents',
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

setupdict['dependency_links'] = ['http://ooici.net/packages']
setupdict['test_suite'] = 'cei'
setupdict['install_requires'] = ['ioncore==0.4.3']
#setupdict['include_package_data'] = True
#setupdict['package_data'] = {
#    'cei': ['data/*.sqlt', 'data/install.sh']
setupdict['install_requires'] = ['simplejson==2.1.2', 
                                 'httplib2==0.6.0',
                                 'nimboss',
                                 'txrabbitmq==0.4',
                                 'apache-libcloud==0.4.0']

# Newest ioncore not packaged yet, use git HEAD
# 'ioncore==0.4.3']

setup(**setupdict)
