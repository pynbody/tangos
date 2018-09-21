from __future__ import absolute_import
install_requires = [
    'setuptools',
    'numpy >= 1.10.0',
    'sqlalchemy >= 1.0.10',
    'pyparsing >= 2.1.0',
    'WebOb >= 1.7.0rc2', # Response.has_body
    'repoze.lru >= 0.4', # py3 compat
    'zope.interface >= 3.8.0',  # has zope.interface.registry
    'zope.deprecation >= 3.5.0', # py3 compat
    'venusian >= 1.0a3', # ``ignore``
    'translationstring >= 0.4', # py3 compat
    'PasteDeploy >= 1.5.0', # py3 compat
    'plaster',
    'plaster_pastedeploy',
    'pyramid',
    'waitress',
    'pyramid_debugtoolbar',
    'pyramid_jinja2',
    'pyramid_tm','pyramid_retry',
    'zope.sqlalchemy',
    'hupper',
    'six',
    'scipy >= 0.14.0'
    ]

tests_require = [
    'nose >= 1.3.0',
    'webtest >= 2.0',
    'pyquery >= 1.3.0',
    'pynbody >= 0.46',
    'yt>=3.4.0'
    ]

from setuptools import setup, find_packages


setup(name='tangos',
      version='1.0.9',
      description='TANGOS, the agile numerical galaxy organisation system',
      classifiers=[
          "Development Status :: 4 - Beta",
          "Intended Audience :: Developers",
          "Programming Language :: Python",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "License :: OSI Approved :: BSD License",
      ],
      author="Andrew Pontzen",
      author_email="a.pontzen@ucl.ac.uk",
      license="GNUv3",
      packages=find_packages(),
      entry_points={'paste.app_factory': [
                                            'main = tangos.web:main',
                                            ],
                    'console_scripts': [   'tangos_add_bh = tangos.scripts.add_bh:main',
                                           'tangos_bh_timelink = tangos.scripts.bh_timelink:main',
                                           'tangos_crosslink = tangos.scripts.crosslink:main',
                                           'tangos_fix_bh_hosts = tangos.scripts.fix_bh_hosts:main',
                                           'tangos_import_from_ahf = tangos.scripts.import_from_ahf:main',
                                           'tangos_manager = tangos.scripts.manager:main',
                                           'tangos_preprocess_bh = tangos.scripts.preprocess_bh:main',
                                           'tangos_timelink = tangos.scripts.timelink:main',
                                           'tangos_writer = tangos.scripts.writer:main',
                                           'tangos = tangos.scripts:main'
                    ]
      },
      include_package_data=True,
      zip_safe=False,
      python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
      install_requires=install_requires,
      tests_require=tests_require,
      test_suite="nose.collector"
      )
