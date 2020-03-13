import setuptools

# with open("README.md", "r") as fh:
#     long_description = fh.read()

setuptools.setup(
    name="tcia_downloader",
    version="0.0.1",
    author="Théophraste HENRY",
    author_email="theophraste.henry@gmail.com",
    description="TODO",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    licence="Apache License 2.0",
    url="https://github.com/lescientifik/tcia_downloader",
    packages=setuptools.find_packages(),
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
    ],
    python_requires=">=3.7",
    entry_points={"console_scripts": ["tcia_dl = tcia_downloader.main:download"]},
)
