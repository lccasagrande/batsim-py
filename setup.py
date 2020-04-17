from os import path

from setuptools import find_packages, setup


def read_requirements_file(filename):
    file = '%s/%s' % (path.dirname(path.realpath(__file__)), filename)
    with open(file) as f:
        return [line.strip() for line in f]


with open("batsim_py/__version__.py") as version_file:
    exec(version_file.read())


setup(
    name='batsim-py',
    author='lccasagrande',
    version=__version__,
    license="MIT",
    python_requires='>=3.7',
    install_requires=read_requirements_file('requirements.txt'),
    packages=find_packages(),
    package_dir={'batsim_py': 'batsim_py'},
)
