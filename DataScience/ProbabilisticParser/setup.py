try:
    from setuptools import setup
except ImportError :
    raise ImportError("setuptools module required, please go to https://pypi.python.org/pypi/setuptools and follow the instructions for installing setuptools")

setup(version='0.1',
      url='https://github.com/ONSdigital/address-index-data/tree/master/DataScience/ProbabilisticParser',
      description='Probabilistic Address Parser using Conditional Random Fields',
      author='Sami Niemi',
      author_email=['sami.niemi@valtech.co.uk', 'sami-matias.niemi@ons.gov.uk'],
      name='addressParser',
      packages=['addressParser'],
      license='TBC',
      install_requires=['python-crfsuite>=0.7', 'lxml'],
      classifiers=['Development Status :: 3 - Alpha',
                   'Intended Audience :: Developers',
                   'Intended Audience :: Science/Research',
                   'License :: TBC',
                   'Natural Language :: English',
                   'Operating System :: MacOS :: MacOS X',
                   'Operating System :: Microsoft :: Windows',
                   'Operating System :: POSIX',
                   'Programming Language :: Python :: 3.5',
                   'Programming Language :: Python :: 3 :: Only',
                   'Topic :: Software Development :: Libraries :: Python Modules',
                   'Topic :: Scientific/Engineering',
                   'Topic :: Scientific/Engineering :: Information Analysis'])
