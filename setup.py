from os import path

from setuptools import find_packages, setup


def read_requirements_file(filename):
    file = '%s/%s' % (path.dirname(path.realpath(__file__)), filename)
    with open(file) as f:
        return [line.strip() for line in f]


with open("batsim_py/__version__.py") as version_file:
    exec(version_file.read())


with open("README.rst") as readme_file:
    long_description = readme_file.read()


install_requires = read_requirements_file('requirements.txt')
docs_requires = read_requirements_file('docs/requirements.txt')
tests_requires = read_requirements_file('requirements-dev.txt')
dev_requires = read_requirements_file('requirements-dev.txt')
setup_requires = ['pytest-runner']

setup(
    name='batsim-py',
    version=__version__,
    author='lccasagrande',
    author_email='lcamelocasagrande@gmail.com',
    url='https://github.com/lccasagrande/batsim-py',
    download_url=f'https://github.com/lccasagrande/batsim-py/archive/v{__version__}.tar.gz',
    project_urls={
        'Documentation': 'https://lccasagrande.github.io/batsim-py/',
        'Tracker': 'https://github.com/lccasagrande/batsim-py/issues',
    },
    license='MIT',
    description="Batsim-py allows using Batsim from Python 3.",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    python_requires='>=3.8',
    install_requires=install_requires,
    tests_require=tests_requires,
    setup_requires=setup_requires,
    extras_require={
        'docs': docs_requires,
        'dev': dev_requires,
    },
    packages=find_packages(),
    package_dir={'batsim_py': 'batsim_py'},
    keywords=["Cluster", "Scheduler", "Resource and Job Management"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Topic :: System :: Clustering",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
    ],
)
