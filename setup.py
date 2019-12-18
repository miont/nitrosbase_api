import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="nitrosbase", # Replace with your own username
    version="0.0.1",
    author="Konstantin Ostrovskiy",
    author_email="kostrovskiy@fil-it.ru",
    description="Nitrosbase Python API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/miont/nitrosbase_api",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)