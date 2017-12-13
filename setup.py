from distutils.core import setup
setup(name='ods_adapter',
      version='0.0.1',
      py_modules=['ods_adapter'],
      requires=['yaml', 'bosh_api', 'jsonpath_ng']
      )
