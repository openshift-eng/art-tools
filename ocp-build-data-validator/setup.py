import setuptools

with open('./requirements.txt') as f:
    INSTALL_REQUIRES = f.read().splitlines()

setuptools.setup(
    name='rh-ocp-build-data-validator',
    author='AOS ART Team',
    author_email='aos-team-art@redhat.com',
    version='0.2.5',
    description='Validation of ocp-build-data Image & RPM declarations',
    long_description_content_type='text/x-rst',
    long_description=open('README.rst').read(),
    url='https://github.com/openshift/ocp-build-data-validator',
    license='Apache License, Version 2.0',
    packages=['validator', 'validator.schema'],
    entry_points={'console_scripts': [
        'validate-ocp-build-data = validator.__main__:main',
    ]},
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
)
