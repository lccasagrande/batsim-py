from setuptools import setup

setup(name='batsim_py',
      author='lccasagrande',
      version='0.1',
      python_requires='>=3.7',
      install_requires=[
              'zmq',
              'procset',
              'pydispatcher',
              'evalys'
      ])
