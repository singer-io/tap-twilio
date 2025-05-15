#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-twilio',
      version='0.2.0',
      description='Singer.io tap for extracting data from the Twilio API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_twilio'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.32.3',
          'singer-python==5.13.0'
      ],
      entry_points='''
          [console_scripts]
          tap-twilio=tap_twilio:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_twilio': [
              'schemas/*.json'
          ]
      })
