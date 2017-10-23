try:
    from setuptools import setup
except ImportError:
    print("setuptools missing falling back to distutils")
    from distutils.core import setup

setup(
    name='IBInterface',
    version='0.1',
    packages=['ib_interface'],
    url='',
    license='',
    author='speedab',
    author_email='',
    description=''
)
