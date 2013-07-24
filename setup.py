from distutils.core import setup
import os

longdesc = \
'''A library to manage a relational database containing NFL data, including play-by-play statistics. The library also includes several scripts for maintaining and updating your database, including monitoring live games and keeping statistics up to date in near-realtime.'''

try:
    docfiles = map(lambda s: 'doc/%s' % s, list(os.walk('doc'))[0][2])
except IndexError:
    docfiles = []

setup(
    name='nfldb',
    author='Andrew Gallant',
    author_email='nfldb@burntsushi.net',
    version='0.0.1',
    license='WTFPL',
    description='A library to manage and update NFL data in a relational '
                'database.',
    long_description=longdesc,
    url='https://github.com/BurntSushi/nfldb',
    classifiers=[
        'License :: Public Domain',
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Other Audience',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    ],
    platforms='ANY',
    packages=['nfldb'],
    package_dir={'nfldb': 'nfldb'},
    # package_data={'nfldb': ['schedule-status', 'pbp-xml/*.xml.gz']}, 
    data_files=[('share/doc/nfldb', ['README.md', 'COPYING', 'INSTALL']),
                ('share/doc/nfldb/doc', docfiles)],
    install_requires=['nflgame', 'toml'],
    scripts=[]
)
