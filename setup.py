import setuptools

NAME = "influxconnector"

DEPENDENCIES_ARTIFACTORY = [
    'influxdb',
]

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

setuptools.setup(
    name=NAME,
    # version_format='{tag}.dev{commitcount}+{gitsha}',
    version_config=True,
    author="srw2ho",
    author_email="",
    description="",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    package_data={},
    setup_requires=[
        'setuptools-git-version',
    ],
    install_requires=DEPENDENCIES_ARTIFACTORY,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License"
        "Operating System :: OS Independent",
    ],
)
