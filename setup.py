from setuptools import setup

setup(name='batsim-py',
      author='lccasagrande',
      version='0.1',
      python_requires='>=3.6',
      install_requires=[
              'numpy',
              'sortedcontainers',
              'zmq',
              'procset',
              'evalys==4.0.4'
      ])
