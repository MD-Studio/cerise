import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

REQUIRES = ["connexion"]

setup(
    name = "cerise",
    version = "develop",
    author = "Lourens Veen",
    author_email = "l.veen@esciencecenter.nl",
    description = ("A simple CWL job running service using Xenon as a backend"),
    license = "Apache 2.0",
    keywords = ["Python", "CWL", "Xenon"],
    url = "https://github.com/LourensVeen/cerise",
    packages=['cerise'],
    packages_data={'': ['swagger_server/swagger/swagger.yaml']},
    include_package_data=True,
    long_description=read('README.md'),
    classifiers=[
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
    ],
)

