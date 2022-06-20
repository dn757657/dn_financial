from distutils.core import setup

setup(name='dn_financial_pipelines',
      version='0.0.6',
      packages=['dn_crypto', 'dn_pricing', 'dn_qtrade', 'dn_date_util'],
      license='MIT',
      description='data pipelines',
      author='Dan',
      author_email='daniel.js.campbell@gmail.com',
      url='https://github.com/dn757657/dn_financial.git',
      download_url='https://github.com/dn757657/dn_financial/archive/refs/tags/v0.0.6.tar.gz',
      keywords=['docopt', 'sqlite', 'ct-finance'],
      install_requires=[
            'web3~=5.26.0',
            'hexbytes~=0.2.2',
            'qtrade~=0.4.0',
            'pathlib~=1.0.1',
            'pandas~=1.3.5',
            'python-dateutil~=2.8.2'
      ],
      classifiers=[
            'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
            'Intended Audience :: Developers',      # Define that your audience are developers
            'Topic :: Software Development :: Build Tools',
            'License :: OSI Approved :: MIT License',   # Again, pick a license
            'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
      ],
      )
