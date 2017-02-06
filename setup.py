import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "simple-cwl-xenon-service",
    version = "0.0.1",
    author = "Lourens Veen",
    author_email = "l.veen@esciencecenter.nl",
    description = ("An empty Python project"),
    license = "Apache 2.0",
    keywords = "Python",
    url = "https://github.com/LourensVeen/simple-cwl-xenon-service",
    packages=['simple-cwl-xenon-service'],
    long_description=read('README.md'),
    classifiers=[
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
    ],
)

